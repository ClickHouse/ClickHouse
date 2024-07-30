#!/usr/bin/env python

import logging
import os
from os import path as p
from typing import Tuple

from build_download_helper import APIException, get_gh_api

module_dir = p.abspath(p.dirname(__file__))
git_root = p.abspath(p.join(module_dir, "..", ".."))

ROOT_DIR = git_root
IS_CI = bool(os.getenv("CI"))
TEMP_PATH = os.getenv("TEMP_PATH", p.abspath(p.join(module_dir, "./tmp")))
REPORT_PATH = f"{TEMP_PATH}/reports"
# FIXME: latest should not be used in CI, set temporary for transition to "docker with digest as a tag"
DOCKER_TAG = os.getenv("DOCKER_TAG", "latest")
CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH", "")
GITHUB_JOB = os.getenv("GITHUB_JOB_OVERRIDDEN", "") or os.getenv("GITHUB_JOB", "local")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "0")
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_UPSTREAM_REPOSITORY = os.getenv(
    "GITHUB_UPSTREAM_REPOSITORY", "ClickHouse/ClickHouse"
)
GITHUB_WORKSPACE = os.getenv("GITHUB_WORKSPACE", git_root)
GITHUB_RUN_URL = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
IMAGES_PATH = os.getenv("IMAGES_PATH", TEMP_PATH)
REPO_COPY = os.getenv("REPO_COPY", GITHUB_WORKSPACE)
RUNNER_TEMP = os.getenv("RUNNER_TEMP", p.abspath(p.join(module_dir, "./tmp")))
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "clickhouse-builds")
S3_BUILDS_BUCKET_PUBLIC = "clickhouse-builds"
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "clickhouse-test-reports")
S3_URL = os.getenv("S3_URL", "https://s3.amazonaws.com")
S3_DOWNLOAD = os.getenv("S3_DOWNLOAD", S3_URL)
S3_ARTIFACT_DOWNLOAD_TEMPLATE = (
    f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/"
    "{pr_or_release}/{commit}/{build_name}/{artifact}"
)
CI_CONFIG_PATH = f"{TEMP_PATH}/ci_config.json"

# These parameters are set only on demand, and only once
_GITHUB_JOB_ID = ""
_GITHUB_JOB_URL = ""
_GITHUB_JOB_API_URL = ""


def GITHUB_JOB_ID(safe: bool = True) -> str:
    global _GITHUB_JOB_ID
    global _GITHUB_JOB_URL
    global _GITHUB_JOB_API_URL
    if _GITHUB_JOB_ID:
        return _GITHUB_JOB_ID
    try:
        _GITHUB_JOB_ID, _GITHUB_JOB_URL, _GITHUB_JOB_API_URL = get_job_id_url(
            GITHUB_JOB
        )
    except APIException as e:
        logging.warning("Unable to retrieve the job info from GH API: %s", e)
        if not safe:
            raise e
    return _GITHUB_JOB_ID


def GITHUB_JOB_URL(safe: bool = True) -> str:
    try:
        GITHUB_JOB_ID()
    except APIException:
        if safe:
            logging.warning("Using run URL as a fallback to not fail the job")
            return GITHUB_RUN_URL
        raise

    return _GITHUB_JOB_URL


def GITHUB_JOB_API_URL(safe: bool = True) -> str:
    GITHUB_JOB_ID(safe)
    return _GITHUB_JOB_API_URL


def get_job_id_url(job_name: str) -> Tuple[str, str, str]:
    job_id = ""
    job_url = ""
    job_api_url = ""
    if GITHUB_RUN_ID == "0":
        job_id = "0"
    if job_id:
        return job_id, job_url, job_api_url
    jobs = []
    page = 1
    while not job_id:
        response = get_gh_api(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/"
            f"actions/runs/{GITHUB_RUN_ID}/jobs?per_page=100&page={page}"
        )
        page += 1
        data = response.json()
        jobs.extend(data["jobs"])
        for job in data["jobs"]:
            if job["name"] != job_name:
                continue
            job_id = job["id"]
            job_url = job["html_url"]
            job_api_url = job["url"]
            return job_id, job_url, job_api_url
        if (
            len(jobs) >= data["total_count"]  # just in case of inconsistency
            or len(data["jobs"]) == 0  # if we excided pages
        ):
            job_id = "0"

    if not job_url:
        # This is a terrible workaround for the case of another broken part of
        # GitHub actions. For nested workflows it doesn't provide a proper job_name
        # value, but only the final one. So, for `OriginalJob / NestedJob / FinalJob`
        # full name, job_name contains only FinalJob
        matched_jobs = []
        for job in jobs:
            nested_parts = job["name"].split(" / ")
            if len(nested_parts) <= 1:
                continue
            if nested_parts[-1] == job_name:
                matched_jobs.append(job)
        if len(matched_jobs) == 1:
            # The best case scenario
            job_id = matched_jobs[0]["id"]
            job_url = matched_jobs[0]["html_url"]
            job_api_url = matched_jobs[0]["url"]
            return job_id, job_url, job_api_url
        if matched_jobs:
            logging.error(
                "We could not get the ID and URL for the current job name %s, there "
                "are more than one jobs match it for the nested workflows. Please, "
                "refer to https://github.com/actions/runner/issues/2577",
                job_name,
            )

    return job_id, job_url, job_api_url
