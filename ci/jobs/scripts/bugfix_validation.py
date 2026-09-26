from pathlib import Path

from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils

# Build types whose master-HEAD binaries the bugfix-validation runners download
# from S3. The set must match the runner architecture: an x86 binary cannot be
# executed on an ARM runner (and vice versa), so the master-HEAD side of the
# check would fail to install. `BUGFIX_BUILD_TYPES` is the x86 default; pick the
# matching set with `bugfix_build_types(job_name)`.
BUGFIX_BUILD_TYPES = ["amd_asan_ubsan", "amd_tsan", "amd_msan", "amd_debug"]
BUGFIX_BUILD_TYPES_ARM = ["arm_asan_ubsan", "arm_tsan", "arm_msan", "arm_debug"]


def bugfix_build_types(job_name):
    """Select the build-type set matching the job's runner architecture.

    The per-arch jobs carry the arch in their name ("..., aarch64" /
    "..., amd64"). aarch64 jobs run on ARM runners, so they must download the
    ARM master-HEAD binaries; everything else defaults to x86.
    """
    if "aarch64" in job_name:
        return BUGFIX_BUILD_TYPES_ARM
    return BUGFIX_BUILD_TYPES


def find_master_builds(build_types=None):
    """Find S3 URLs for all build types from a recent master commit.

    Verifies that artifacts for every requested build type exist before
    returning, so that a commit with a partial build set is skipped.
    """
    build_types = build_types if build_types is not None else BUGFIX_BUILD_TYPES
    commits = Info().get_kv_data("master_commits") or []
    # Artifacts live under the normalized workflow name:
    #   REFs/master/<sha>/<workflow>/build_<bt>/clickhouse
    workflow = Utils.normalize_string("MasterCI")
    for sha in commits:
        urls = {
            bt: f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/{workflow}/build_{bt}/clickhouse"
            for bt in build_types
        }
        # curl's native --retry only retries transient failures (5xx, timeouts,
        # refused connections), not a genuine 404, so a partially-built older
        # commit is still skipped fast while a transient S3 5xx on the newest
        # commit is retried instead of silently falling back to an older binary.
        if all(
            Shell.check(f"curl -sfI --retry 5 --retry-connrefused {url} > /dev/null")
            for url in urls.values()
        ):
            return urls
    return None


def download_master_builds(build_urls, bt_paths, is_local_run=False):
    """Download the reference master-HEAD binaries listed in `build_urls`.

    Retries each download so a transient S3 5xx does not abort the whole job;
    `find_master_builds` already probed that the artifact exists, so a failure
    here is treated as transient. Still strict: a persistent failure raises
    after exhausting retries. Shared by the functional- and integration-test
    bugfix-validation callers so the retry lives in one place.
    """
    for bt, url in build_urls.items():
        bt_path = bt_paths[bt]
        if not is_local_run or not Path(bt_path).is_file():
            print(f"NOTE: Downloading {bt} build to [{bt_path}]")
            Shell.run(
                f"wget -nv -O {bt_path} {url}", verbose=True, strict=True, retries=5
            )
            Shell.run(f"chmod +x {bt_path}", verbose=True)
