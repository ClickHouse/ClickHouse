import argparse
import dataclasses
import json
import logging
import os
from typing import List

from get_robot_token import get_best_robot_token
from github_helper import GitHub
from ssh import SSHKey
from ci_utils import Shell
from env_helper import GITHUB_REPOSITORY
from report import SUCCESS

LOGGER_NAME = __name__
HELPER_LOGGERS = ["github_helper", LOGGER_NAME]
logger = logging.getLogger(LOGGER_NAME)


def parse_args():
    parser = argparse.ArgumentParser(
        "Checks if enough days elapsed since the last release on each release "
        "branches and do a release in case for green builds."
    )
    parser.add_argument("--token", help="GitHub token, if not set, used from smm")

    return parser.parse_args()


MAX_NUMBER_OF_COMMITS_TO_CONSIDER_FOR_RELEASE = 5
AUTORELEASE_INFO_FILE = "/tmp/autorelease_info.json"


@dataclasses.dataclass
class ReleaseParams:
    release_branch: str
    commit_sha: str


@dataclasses.dataclass
class AutoReleaseInfo:
    releases: List[ReleaseParams]

    def add_release(self, release_params: ReleaseParams):
        self.releases.append(release_params)

    def dump(self):
        print(f"Dump release info into [{AUTORELEASE_INFO_FILE}]")
        with open(AUTORELEASE_INFO_FILE, "w", encoding="utf-8") as f:
            print(json.dumps(dataclasses.asdict(self), indent=2), file=f)


def main():
    args = parse_args()

    token = args.token or get_best_robot_token()
    assert len(token) > 10
    os.environ["GH_TOKEN"] = token
    (Shell.run("gh auth status", check=True))
    gh = GitHub(token)
    prs = gh.get_release_pulls(GITHUB_REPOSITORY)
    branch_names = [pr.head.ref for pr in prs]

    print(f"Found release branches [{branch_names}]")
    repo = gh.get_repo(GITHUB_REPOSITORY)

    autoRelease_info = AutoReleaseInfo(releases=[])
    for pr in prs:
        print(f"Checking PR [{pr.head.ref}]")

        refs = list(repo.get_git_matching_refs(f"tags/v{pr.head.ref}"))
        refs.sort(key=lambda ref: ref.ref)

        latest_release_tag_ref = refs[-1]
        latest_release_tag = repo.get_git_tag(latest_release_tag_ref.object.sha)
        commit_num = int(
            Shell.run(
                f"git rev-list --count {latest_release_tag.tag}..origin/{pr.head.ref}",
                check=True,
            )
        )
        print(
            f"Previous release is [{latest_release_tag}] was [{commit_num}] commits before, date [{latest_release_tag.tagger.date}]"
        )
        commit_reverse_index = 0
        commit_found = False
        commit_checked = False
        commit_sha = ""
        while (
            commit_reverse_index < commit_num - 1
            and commit_reverse_index < MAX_NUMBER_OF_COMMITS_TO_CONSIDER_FOR_RELEASE
        ):
            commit_checked = True
            commit_sha = Shell.run(
                f"git rev-list --max-count=1 --skip={commit_reverse_index} origin/{pr.head.ref}",
                check=True,
            )
            print(
                f"Check if commit [{commit_sha}] [{pr.head.ref}~{commit_reverse_index}] is ready for release"
            )
            commit_reverse_index += 1

            cmd = f"gh api -H 'Accept: application/vnd.github.v3+json' /repos/{GITHUB_REPOSITORY}/commits/{commit_sha}/status"
            ci_status_json = Shell.run(cmd, check=True)
            ci_status = json.loads(ci_status_json)["state"]
            if ci_status == SUCCESS:
                commit_found = True
            break
        if commit_found:
            print(
                f"Add release ready info for commit [{commit_sha}] and release branch [{pr.head.ref}]"
            )
            autoRelease_info.add_release(
                ReleaseParams(release_branch=pr.head.ref, commit_sha=commit_sha)
            )
        else:
            print(f"WARNING: No good commits found for release branch [{pr.head.ref}]")
            if commit_checked:
                print(
                    f"ERROR: CI is failed. check CI status for branch [{pr.head.ref}]"
                )

    autoRelease_info.dump()


if __name__ == "__main__":
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            main()
    else:
        main()
