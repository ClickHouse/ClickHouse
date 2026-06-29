from ci.praktika.info import Info
from ci.praktika.utils import Shell

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
    for sha in commits:
        urls = {
            bt: f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/build_{bt}/clickhouse"
            for bt in build_types
        }
        if all(
            Shell.check(f"curl -sfI {url} > /dev/null") for url in urls.values()
        ):
            return urls
    return None
