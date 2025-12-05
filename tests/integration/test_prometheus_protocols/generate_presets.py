#!/usr/bin/env python3

# This is a helper utility, it downloads presets of time series from "demo.promlabs.com"
# and put them to the "presets" folder.

import os
import time
import zipfile
from prometheus_test_utils import *


def download_preset(preset_name, host, port, path, query, timestamp=None):
    if not timestamp:
        timestamp = round(time.time(), 3)
    print(f"Downloading preset {preset_name} (evaluation_time={timestamp})")
    data = execute_query_via_http_api(host, port, path, query, timestamp=timestamp)
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
    #download_preset(
    #    "demo.promlabs.com-30s.zip",
    #    "demo.promlabs.com",
    #    80,
    #    "api/v1/query",
    #    '{__name__=~".+"}[30s]',
    #)

    download_preset(
        "cheat_sheet.zip",
        "demo.promlabs.com",
        80,
        "api/v1/query",
        '{__name__=~"node_memory_MemFree_bytes|node_memory_Cached_bytes|demo_cpu_usage_seconds_total|demo_num_cpus|node_filesystem_avail_bytes|node_exporter_build_info|go_goroutines|go_threads|up|node_network_mtu_bytes|node_network_address_assign_type"}[30s]',
    )




# MAIN
if __name__ == "__main__":
    download_presets()
