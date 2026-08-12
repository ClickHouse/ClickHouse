from ci.praktika.info import Info
from ci.praktika.utils import Shell

BUGFIX_BUILD_TYPES = ["amd_asan_ubsan", "amd_tsan", "amd_msan", "amd_debug"]


def find_master_builds():
    """Find S3 URLs for all build types from a recent master commit.

    Verifies that artifacts for every entry in BUGFIX_BUILD_TYPES exist
    before returning, so that a commit with a partial build set is skipped.
    """
    commits = Info().get_kv_data("master_commits") or []
    for sha in commits:
        urls = {
            bt: f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/build_{bt}/clickhouse"
            for bt in BUGFIX_BUILD_TYPES
        }
        if all(
            Shell.check(f"curl -sfI {url} > /dev/null") for url in urls.values()
        ):
            return urls
    return None
