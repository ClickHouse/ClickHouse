#!/usr/bin/env python3

import subprocess
import requests
import os
import time

FNAME_START = "+++"

CLOUDFLARE_URL = "https://api.cloudflare.com/client/v4/zones/4fc6fb1d46e87851605aa7fa69ca6fe0/purge_cache"

# we have changes in revision and commit sha on all pages
# so such changes have to be ignored
MIN_CHANGED_WORDS = 4


def collect_changed_files():
    proc = subprocess.Popen("git diff HEAD~1 --word-diff=porcelain | grep -e '^+[^+]\|^\-[^\-]\|^\+\+\+'", stdout=subprocess.PIPE, shell=True)
    changed_files = []
    current_file_name = ""
    changed_words = []
    while True:
        line = proc.stdout.readline().decode("utf-8").strip()
        if not line:
            break
        if FNAME_START in line:
            if changed_words:
                if len(changed_words) > MIN_CHANGED_WORDS:
                    changed_files.append(current_file_name)
                changed_words = []
            current_file_name = line[6:]
        else:
            changed_words.append(line)
    return changed_files


def filter_and_transform_changed_files(changed_files, base_domain):
    result = []
    for f in changed_files:
        if f.endswith(".html"):
            result.append(base_domain + f.replace("index.html", ""))
    return result


def convert_to_dicts(changed_files, batch_size):
    result = []
    current_batch = {"files": []}
    for f in changed_files:
        if len(current_batch["files"]) >= batch_size:
            result.append(current_batch)
            current_batch = {"files": []}
        current_batch["files"].append(f)

    if current_batch["files"]:
        result.append(current_batch)
    return result


def post_data(prepared_batches, token):
    headers = {"Authorization": "Bearer {}".format(token)}
    for batch in prepared_batches:
        print(("Pugring cache for", ", ".join(batch["files"])))
        response = requests.post(CLOUDFLARE_URL, json=batch, headers=headers)
        response.raise_for_status()
        time.sleep(3)


if __name__ == "__main__":
    token = os.getenv("CLOUDFLARE_TOKEN")
    if not token:
        raise Exception("Env variable CLOUDFLARE_TOKEN is empty")
    base_domain = os.getenv("BASE_DOMAIN", "https://content.clickhouse.tech/")
    changed_files = collect_changed_files()
    print(("Found", len(changed_files), "changed files"))
    filtered_files = filter_and_transform_changed_files(changed_files, base_domain)
    print(("Files rest after filtering", len(filtered_files)))
    prepared_batches = convert_to_dicts(filtered_files, 25)
    post_data(prepared_batches, token)
