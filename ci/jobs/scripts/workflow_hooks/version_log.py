from datetime import datetime

from praktika.info import Info
from praktika.utils import Shell

from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.clickhouse_version import CHVersion


def _add_build_to_version_history():
    info = Info()
    Shell.check(
        f"git rev-parse --is-shallow-repository | grep -q true && git fetch --unshallow --prune --no-recurse-submodules --filter=tree:0 origin {info.git_branch} ||:"
    )
    commit_parents = Shell.get_output("git log --format=%P -n 1").split(" ")
    data = {
        "check_start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "pull_request_number": info.pr_number,
        "pull_request_url": info.pr_url,
        "commit_sha": info.sha,
        "commit_url": info.commit_url,
        "parent_commits_sha": commit_parents,
        "version": CHVersion.get_version(),
        "git_ref": info.git_branch,
    }
    print(f"Update version log: [{data}]")
    CIDBCluster().insert_json(table="version_history", json_str=data)
    # stores actual version data in pipline storage, to be used by jobs that need it
    CHVersion.store_version_data_in_ci_pipeline()


if __name__ == "__main__":
    _add_build_to_version_history()
