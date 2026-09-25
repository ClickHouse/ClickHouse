#!/usr/bin/env python3
# `--resolve` (workflow pre-hook): picks the tested and reference master builds, stores their SHAs in KV data.
# Job mode: downloads those builds and execs the performance harness (or ClickBench with `--clickbench`).

import argparse
import os

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.jobs.scripts.workflow_hooks.store_data import MASTER_TRACK_COMMITS, get_master_first_parent_commits
from ci.praktika._environment import _Environment
from ci.praktika.info import Info
from ci.praktika.settings import Settings
from ci.praktika.utils import Shell

BUILD_TYPE = "build_arm_release"
TESTED_PATH = "./ci/tmp/clickhouse"
REFERENCE_PATH = "./ci/tmp/reference/clickhouse"


def build_urls(sha):
    """The `Build (arm_release)` binary of master commit `sha`, newest layout first: the two layouts of
    `master_build_links` in `performance_tests.py`, which the pre-hook does not import (the harness module pulls in
    the dependencies of the job image)."""
    prefix = _Environment.get_s3_prefix_static(pr_number=0, branch="master", sha=sha, workflow_name="MasterCI")
    return [f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/{p}/{BUILD_TYPE}/clickhouse" for p in (prefix, f"REFs/master/{sha}")]


def published_url(sha):
    return next((url for url in build_urls(sha) if Shell.check(f"curl -sfI {url} > /dev/null")), None)


def first_published(shas, what):
    for sha in shas:
        if published_url(sha):
            return sha
    raise RuntimeError(f"No published ARM release build among {len(shas)} commits for the {what}")


def resolve():
    info = Info()
    # The first-parent chain from the trigger commit and the release branch base with its predecessors, computed as
    # `store_data.py` does for master runs only, so that a run from a branch resolves too.
    chain = get_master_first_parent_commits(info.sha, MASTER_TRACK_COMMITS + 1)
    predecessors = Shell.get_output_or_raise(f"git rev-list --max-count=20 '{CHVersion.get_release_version().githash}'").split()
    tested = first_published(chain, "tested binary")
    print(f"tested binary: {tested}")

    # Runs from other branches (`gh workflow run --ref`) write rows too; only master runs are the nightly history.
    previous = CIDBCluster().do_select_query(
        f"""SELECT new_sha FROM perf_test_times_v1
WHERE workflow_name = '{info.workflow_name}' AND baseline_kind = 'master_head' AND pr_number = 0
  AND report_url LIKE '%?REF=master&%' AND check_start_time < toDateTime({int(info.workflow_start_time)})
ORDER BY check_start_time DESC LIMIT 1""",
        timeout=Settings.CI_DB_QUERY_TIMEOUT_SEC,
    )
    if previous is None:
        raise RuntimeError("Previous nightly lookup in CIDB failed")
    master_head = previous.strip()
    if not master_head:
        master_head = first_published(chain[chain.index(tested) + 1 :], "bootstrap reference")
        print("bootstrap: no previous nightly run found; reference = previous published master build")
    elif master_head == tested:
        print("A/A: tested == reference")
    print(f"master_head reference: {master_head}")

    release_base = first_published(predecessors, "release base")
    print(f"release_base reference: {release_base}")

    info.store_kv_data("nightly_tested_sha", tested)
    info.store_kv_data("nightly_reference_sha_master_head", master_head)
    info.store_kv_data("nightly_reference_sha_release_base", release_base)


def download(sha, path):
    url = published_url(sha)
    if not url:
        raise RuntimeError(f"No published ARM release build for {sha}")
    Shell.check(f"mkdir -p {os.path.dirname(path)} && wget -nv -O {path} {url} && chmod +x {path}", strict=True, verbose=True)


def run_job(args, rest):
    info = Info()
    print(Shell.get_output_or_raise(
        "uname -r && grep PRETTY_NAME /etc/os-release && lscpu | grep -E '^(Model name|CPU\\(s\\)):'"
        " && grep MemTotal /proc/meminfo"))
    print(f"instance: {info.instance_type} {info.instance_id}")

    kv = info.get_kv_data()
    tested = kv["nightly_tested_sha"]
    download(tested, TESTED_PATH)
    if args.clickbench:
        os.execvp("python3", ["python3", "./ci/jobs/clickbench.py", "--info", f"binary {tested}"])

    (mode,) = {o.strip() for o in args.test_options.split(",")} & {"master_head", "release_base"}
    download(kv[f"nightly_reference_sha_{mode}"], REFERENCE_PATH)
    os.execvp("python3", ["python3", "./ci/jobs/performance_tests.py", "--test-options", args.test_options,
                          "--reference-path", REFERENCE_PATH, *rest])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nightly extended performance comparison", allow_abbrev=False)
    parser.add_argument("--resolve", action="store_true")
    parser.add_argument("--test-options", default="")
    parser.add_argument("--clickbench", action="store_true")
    args, rest = parser.parse_known_args()
    if args.resolve:
        resolve()
    else:
        run_job(args, rest)
