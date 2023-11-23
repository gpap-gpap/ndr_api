# -*- coding: utf-8 -*-
# Copyright Giorgos Papageorgiou
# License: Apache 2.0
"""
    _summary_

_extended_summary_

Raises:
    Exception: _description_

Returns:
    _type_: _description_
"""

from __future__ import annotations
import os
import requests
import json
from bs4 import BeautifulSoup
from functools import cached_property
from typing import Literal, Union, Optional, List, Dict, Any
import pandas as pd
import numpy as np

f2m = 0.3048


class NDRrequests:
    api_info = {
        "site id": os.getenv("NDR_API_SITE_ID"),
        "client id": os.getenv("NDR_API_CLIENT_ID"),
        "client secret": os.getenv("NDR_API_CLIENT_SECRET"),
        "tenant id": os.getenv("NDR_API_TENANT_ID"),
        "lists": {
            "project id": os.getenv("NDR_API_PROJECT_ID"),
            "file id": os.getenv("NDR_API_FILE_ID"),
            # below are not necessary
            # "completeness mhaz":os.getenv("NDR_API_COMPLETENESS_MHAZ"),
            # "completeness seis":os.getenv("NDR_API_COMPLETENESS_SEIS"),
            # "completeness well":os.getenv("NDR_API_COMPLETENESS_WELL"),
            # "completeness rems":os.getenv("NDR_API_COMPLETENESS_REMS"),
            # "completeness intp":os.getenv("NDR_API_COMPLETENESS_INTP"),
            # "company name":os.getenv("NDR_API_COMPANY_NAME"),
            # "completeness comments":os.getenv("NDR_API_COMPLETENESS_COMMENTS")
        },
    }

    data = {
        "client_id": api_info["client id"],
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
        "client_secret": api_info["client secret"],
    }

    token_request = (
        "https://login.microsoftonline.com/"
        + api_info["tenant id"]
        + "/oauth2/v2.0/token"
    )

    def __init__(self, proxies: dict = None):
        for key, value in NDRrequests.api_info.items():
            # check if all keys have been exported as environment variables
            # and they are accessible and not empty
            if not isinstance(value, dict):
                assert value is not None, f"expected {key} to be set"
            else:
                for key2, value2 in value.items():
                    assert value2 is not None, f"expected {key2} to be set"
        print("API keys successfully read")
        if proxies is None:
            self.session = requests.Session()
        else:
            self.session = requests.Session()
            self.session.proxies.update(proxies)
        self.response = self.session.post(
            NDRrequests.token_request,
            data=NDRrequests.data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if self.response.status_code == 200:  # successfully got access token
            self._access_token = self.response.json()["access_token"]
            print("access token was retrieved successfully")
        else:  # failed to get access token
            print("There was an error getting the access token")
            print(self.response.status_code, self.response.json())
        self._current_url = None

    @property
    def headers(self):
        if not self.access_token:
            print(
                "access token does not exist, either server is down or init didn't work"
            )
        else:
            headers = {
                "Authorization": "Bearer " + self.access_token,
                "Content-Type": "application/json",
            }
        return headers

    @property
    def access_token(self):
        return self._access_token

    def reset_current_url(self):
        self._current_url = None

    @property
    def current_url(self):
        if self._current_url is None:
            self._current_url = ""
        return self._current_url

    @current_url.setter
    def current_url(self, value) -> None:
        self._current_url = value

    def _return_response(
        self, url: str, headers: dict = None
    ) -> Union[Dict[str, Any], None]:
        if headers is None:
            headers = self.headers
        response = self.session.get(url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            print(
                f"failed to get response from server - got status code:{response.status_code} and json:{response.json()}"
            )
            return None

    def create_url(self, list_id: str) -> None:
        try:
            lid = NDRrequests.api_info["lists"][list_id]
            self.reset_current_url()
            self.current_url += (
                "https://graph.microsoft.com/v1.0/sites/"
                + NDRrequests.api_info["site id"]
                + "/lists/"
            )
            self.current_url += lid
        except KeyError:
            print("list id must be one of " + str(NDRrequests.api_info["lists"].keys()))
            self.current_url = None

    def get_key_names(self, list_id: str) -> pd.DataFrame:
        self.create_url(list_id)
        if self.current_url is None:
            response = None
        else:
            self.current_url += "/columns?$select=name,displayName,description"
            unparsed_response = self._return_response(self.current_url)
            response = [
                [i["name"], i["displayName"], i["description"]]
                for i in unparsed_response["value"][1:]
            ]
        if response is not None:
            result = pd.DataFrame(
                response[1:], columns=["name", "displayName", "description"]
            )
        else:
            result = print(f"failed to get keys for {list_id}")
        return result

    def lists_url(self, list_id):
        self.add_sites_url()
        result = self.sites_url + NDRrequests.api_info["lists"][list_id] + "/items"
        return result

    def get_LAS_by_key(
        self, *, key: str = "survid", value: str = None
    ) -> Union[Dict[str, Any], None]:
        if key not in self.get_key_names("file id")["name"].to_list():
            print(f"{key} is not a valid key")
            return None
        self.create_url("file id")
        self.current_url += f"/items?expand=fields(select={key},fnam)&$filter=fields/{key} eq '{value}' and fields/ffmt eq 'LAS'"
        response = self._return_response(self.current_url)
        if response is not None:
            result = {
                val["fields"][key] + f" {i}": val["fields"]["fnam"]
                for i, val in enumerate(response["value"])
            }
        return result

    def get_LAS_by_quadrant(self, quadrant_value: str) -> Union[Dict[str, Any], None]:
        self.create_url("project id")
        self.current_url += f"/items?expand=fields(select=quad,survid,ptyp)&$filter=(fields/quad eq '{quadrant_value}' and fields/ptyp eq 'well')"
        headers = self.headers
        headers["Prefer"] = "HonorNonIndexedQueriesWarningMayFailRandomly"

        response = self._return_response(self.current_url, headers=headers)
        if response is not None:
            result = {
                f"quadrant well {i}": val["fields"]["survid"]
                for i, val in enumerate(response["value"])
            }
        return result

    def bgs_to_ndr(self, string) -> str:
        aux = "- "
        s1, s2 = string.split("-")
        s1 = s1.lstrip("0")
        s2 = s2.lstrip("0")
        result = s1 + aux + s2
        return result

    def ndr_to_bgs(self, string) -> str:
        aux = "-"
        s1, s2 = string.split("-")
        s2 = s2.lstrip()
        aux2 = "/"
        s11, s12 = s1.split("/")

        def add_zeros(string2):
            if len(string2) == 2:
                result = "0" + string2
            elif len(string2) == 1:
                result = "00" + string2
            else:
                result = string2
            return result

        s11 = add_zeros(s11)
        s2 = add_zeros(s2)
        result = s11 + aux2 + s12 + aux + s2
        return result


class BGSTopsScraper:
    base_url = "https://itportal.nstauthority.co.uk/information/well_data/bgs_tops/geological_tops/"
    links = base_url + "seclinks.htm"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }

    def __init__(self):
        self.session = requests.Session()
        self._well_ids = None
        self._well_ids_url = None
        self.model_dictionary = {}

    def get_well_info(self, well_url):
        # download the page
        response = self.session.get(well_url, headers=BGSTopsScraper.headers)

        # check successful response
        if response.status_code != 200:
            print("Status code:", response.status_code)
            raise Exception("Failed to fetch web page ")

        # parse using beautiful soup
        paper_doc = BeautifulSoup(response.text, "html.parser")

        return paper_doc

    def retrieve_well_ids(self) -> list:
        doc = self.get_well_info(BGSTopsScraper.links)
        result = [i.get_text() for i in doc.find_all("a")]
        return result

    def create_ids_url(self) -> list:
        doc = self.get_well_info(BGSTopsScraper.links)
        self._well_ids_url = {
            i.get_text(): BGSTopsScraper.base_url + i.get("href")
            for i in doc.find_all("a")
        }

    @property
    def well_ids_url(self):
        if self._well_ids_url is None:
            self.create_ids_url()
        return self._well_ids_url

    @property
    def well_ids(self):
        if self._well_ids is None:
            self._well_ids = self.retrieve_well_ids()
        return self._well_ids

    def get_pandas_from_well_id(self, well_id):
        try:
            self.model_dictionary[well_id]
        except:
            KeyError
        if well_id not in self.well_ids:
            print("well id must be one of {self.well_ids}")
        else:
            df = pd.read_html(self.well_ids_url[well_id], skiprows=2, header=0)[4]
            model = df[["Top Down Hole Depth"]].apply(
                lambda x: np.round(f2m * x, 1)
            )  # convert feet to meters
            result = model.to_numpy()[:, 0]
            self.model_dictionary[well_id] = np.insert(result, 0, 0)
        return self.model_dictionary[well_id]
