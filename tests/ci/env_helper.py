#!/usr/bin/env python

import os
from os import path as p

module_dir = p.abspath(p.dirname(__file__))
git_root = p.abspath(p.join(module_dir, "..", ".."))

ROOT_DIR = git_root
IS_CI = bool(os.getenv("CI"))
IS_NEW_CI = bool(int(os.getenv("PRAKTIKA", "0")))
# i.e. "ClickHouse/tests/ci/tmp"
TEMP_PATH = os.getenv("TEMP_PATH", p.abspath(p.join(module_dir, "./tmp")))
REPORT_PATH = f"{TEMP_PATH}/reports"
# FIXME: latest should not be used in CI, set temporary for transition to "docker with digest as a tag"
DOCKER_TAG = os.getenv("DOCKER_TAG", "latest")
CACHES_PATH = os.getenv("CACHES_PATH", TEMP_PATH)
CLOUDFLARE_TOKEN = os.getenv("CLOUDFLARE_TOKEN")
GITHUB_EVENT_PATH = os.getenv("GITHUB_EVENT_PATH", "")
GITHUB_JOB = os.getenv("GITHUB_JOB_OVERRIDDEN", "") or os.getenv("GITHUB_JOB", "local")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "Altinity/ClickHouse")
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

S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
S3_BUILDS_BUCKET = os.getenv("S3_BUILDS_BUCKET", "altinity-build-artifacts")
S3_BUILDS_BUCKET_PUBLIC = "altinity-build-artifacts"
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_TEST_REPORTS_BUCKET = os.getenv("S3_TEST_REPORTS_BUCKET", "altinity-build-artifacts")

S3_URL = os.getenv("S3_URL", "https://s3.amazonaws.com")
S3_DOWNLOAD = os.getenv("S3_DOWNLOAD", S3_URL)
S3_ARTIFACT_DOWNLOAD_TEMPLATE = (
    f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/"
    "{pr_or_release}/{commit}/{build_name}/{artifact}"
)
CI_CONFIG_PATH = f"{TEMP_PATH}/ci_config.json"
CLICKHOUSE_TEST_STAT_LOGIN = os.getenv("CLICKHOUSE_TEST_STAT_LOGIN")
CLICKHOUSE_TEST_STAT_PASSWORD = os.getenv("CLICKHOUSE_TEST_STAT_PASSWORD")
CLICKHOUSE_TEST_STAT_URL = os.getenv("CLICKHOUSE_TEST_STAT_URL")
DOCKER_PASSWORD = os.getenv("DOCKER_PASSWORD")
ROBOT_TOKEN = os.getenv("ROBOT_TOKEN")
