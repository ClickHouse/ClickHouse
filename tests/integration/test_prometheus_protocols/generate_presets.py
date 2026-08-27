#!/usr/bin/env python3

# This is a helper utility, it downloads presets of time series from "demo.promlabs.com"
# and put them to the "presets" folder.

import os
import zipfile
from prometheus_test_utils import *


def download_preset(preset_name, host, port, path, query):
    print(f"Downloading preset {preset_name}")
    data = execute_instant_query_with_http_api(host, port, path, query)
    data_bytes = data.encode()
    preset_fullname = os.path.join(PRESETS_DIR, preset_name)
    if preset_fullname.endswith(".zip"):
        with zipfile.ZipFile(
            preset_fullname, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zip:
            with zip.open(
                os.path.splitext(os.path.basename(preset_fullname))[0] + ".json",
                mode="w",
            ) as file:
                file.write(data_bytes)
    else:
        with open(preset_fullname, mode="wb") as file:
            file.write(data_bytes)

    print("Done")


def download_presets():
    download_preset(
        "demo.promlabs.com-30s.zip",
        "demo.promlabs.com",
        80,
        "api/v1/query",
        '{__name__=~".+"}[30s]',
    )


# MAIN
if __name__ == "__main__":
    download_presets()
