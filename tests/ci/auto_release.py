import argparse
import dataclasses
import json
import os
import sys
from typing import List

from ci_buddy import CIBuddy
from ci_config import CI
from ci_utils import Shell
from env_helper import GITHUB_REPOSITORY
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from report import SUCCESS


def parse_args():
    parser = argparse.ArgumentParser(
        "Checks if enough days elapsed since the last release on each release "
        "branches and do a release in case for green builds."
    )
    parser.add_argument("--token", help="GitHub token, if not set, used from smm")
    parser.add_argument(
        "--post-status",
        action="store_true",
        help="Post release branch statuses",
    )
    parser.add_argument(
        "--post-auto-release-complete",
        action="store_true",
        help="Post autorelease completion status",
    )
    parser.add_argument(
        "--prepare",
        action="store_true",
        help="Prepare autorelease info",
    )
    parser.add_argument(
        "--wf-status",
        type=str,
        default="",
        help="overall workflow status [success|failure]",
    )
    return parser.parse_args(), parser


MAX_NUMBER_OF_COMMITS_TO_CONSIDER_FOR_RELEASE = 5
AUTORELEASE_INFO_FILE = "/tmp/autorelease_info.json"
AUTORELEASE_MATRIX_PARAMS = "/tmp/autorelease_params.json"


@dataclasses.dataclass
class ReleaseParams:
    ready: bool
    ci_status: str
    num_patches: int
    release_branch: str
    commit_sha: str
    commits_to_branch_head: int
    latest: bool

    def to_dict(self):
        return dataclasses.asdict(self)


@dataclasses.dataclass
class AutoReleaseInfo:
    releases: List[ReleaseParams]

    def add_release(self, release_params: ReleaseParams) -> None:
        self.releases.append(release_params)

    def dump(self):
        print(f"Dump release info into [{AUTORELEASE_INFO_FILE}]")
        with open(AUTORELEASE_INFO_FILE, "w", encoding="utf-8") as f:
            print(json.dumps(dataclasses.asdict(self), indent=2), file=f)

        # dump file for GH action matrix that is similar to the file above but with dropped not ready release branches
        params = dataclasses.asdict(self)
        params["releases"] = [
            release for release in params["releases"] if release["ready"]
        ]
        with open(AUTORELEASE_MATRIX_PARAMS, "w", encoding="utf-8") as f:
            print(json.dumps(params, indent=2), file=f)

    @staticmethod
    def from_file() -> "AutoReleaseInfo":
        with open(AUTORELEASE_INFO_FILE, "r", encoding="utf-8") as json_file:
            res = json.load(json_file)
        releases = [ReleaseParams(**release) for release in res["releases"]]
        return AutoReleaseInfo(releases=releases)


def _prepare(token):
    assert len(token) > 10
    os.environ["GH_TOKEN"] = token
    Shell.check("gh auth status")

    gh = GitHub(token)
    prs = gh.get_release_pulls(GITHUB_REPOSITORY)
    prs.sort(key=lambda x: x.head.ref)
    branch_names = [pr.head.ref for pr in prs]
    print(f"Found release branches [{branch_names}]")

    repo = gh.get_repo(GITHUB_REPOSITORY)
    autoRelease_info = AutoReleaseInfo(releases=[])

    for pr in prs:
        print(f"\nChecking PR [{pr.head.ref}]")

        refs = list(repo.get_git_matching_refs(f"tags/v{pr.head.ref}"))
        assert refs

        latest_release_tag_ref = refs[-1]
        latest_release_tag = repo.get_git_tag(latest_release_tag_ref.object.sha)

        commits = Shell.get_output_or_raise(
            f"git rev-list --first-parent {latest_release_tag.tag}..origin/{pr.head.ref}",
        ).split("\n")
        commit_num = len(commits)
        if latest_release_tag.tag.endswith("new"):
            print("It's a new release branch - skip auto release for it")
            continue

        print(
            f"Previous release [{latest_release_tag.tag}] was [{commit_num}] commits ago, date [{latest_release_tag.tagger.date}]"
        )

        commits_to_check = commits[:-1]  # Exclude the version bump commit
        commit_sha = ""
        commit_ci_status = ""
        commits_to_branch_head = 0

        for idx, commit in enumerate(
            commits_to_check[:MAX_NUMBER_OF_COMMITS_TO_CONSIDER_FOR_RELEASE]
        ):
            print(
                f"Check commit [{commit}] [{pr.head.ref}~{idx+1}] as release candidate"
            )
            commit_num -= 1

            is_completed = CI.GH.check_wf_completed(token=token, commit_sha=commit)
            if not is_completed:
                print(f"CI is in progress for [{commit}] - check previous commit")
                commits_to_branch_head += 1
                continue

            # TODO: switch to check if CI is entirely green
            statuses = [
                CI.GH.get_commit_status_by_name(
                    token=token,
                    commit_sha=commit,
                    # handle old name for old releases
                    status_name=(CI.JobNames.BUILD_CHECK, "ClickHouse build check"),
                ),
                CI.GH.get_commit_status_by_name(
                    token=token,
                    commit_sha=commit,
                    # handle old name for old releases
                    status_name=CI.JobNames.STATELESS_TEST_RELEASE,
                ),
                CI.GH.get_commit_status_by_name(
                    token=token,
                    commit_sha=commit,
                    # handle old name for old releases
                    status_name=CI.JobNames.STATEFUL_TEST_RELEASE,
                ),
            ]
            commit_sha = commit
            if any(status == SUCCESS for status in statuses):
                commit_ci_status = SUCCESS
                break

            print(f"CI status [{statuses}] - skip")
            commits_to_branch_head += 1

        ready = False
        if commit_ci_status == SUCCESS and commit_sha:
            print(
                f"Add release ready info for commit [{commit_sha}] and release branch [{pr.head.ref}]"
            )
            ready = True
        else:
            print(f"WARNING: No ready commits found for release branch [{pr.head.ref}]")

        autoRelease_info.add_release(
            ReleaseParams(
                release_branch=pr.head.ref,
                commit_sha=commit_sha,
                ready=ready,
                ci_status=commit_ci_status,
                num_patches=commit_num,
                commits_to_branch_head=commits_to_branch_head,
                latest=False,
            )
        )

    if autoRelease_info.releases:
        autoRelease_info.releases[-1].latest = True

    autoRelease_info.dump()


def main():
    args, parser = parse_args()

    if args.post_status:
        info = AutoReleaseInfo.from_file()
        for release_info in info.releases:
            if release_info.ready:
                CIBuddy(dry_run=False).post_info(
                    title=f"Auto Release Status for {release_info.release_branch}",
                    body=release_info.to_dict(),
                )
            else:
                CIBuddy(dry_run=False).post_warning(
                    title=f"Auto Release Status for {release_info.release_branch}",
                    body=release_info.to_dict(),
                )
    elif args.post_auto_release_complete:
        assert args.wf_status, "--wf-status Required with --post-auto-release-complete"
        if args.wf_status != SUCCESS:
            CIBuddy(dry_run=False).post_job_error(
                error_description="Autorelease workflow failed",
                job_name="Autorelease",
                with_instance_info=False,
                with_wf_link=True,
                critical=True,
            )
        else:
            CIBuddy(dry_run=False).post_info(
                title="Autorelease completed",
                body="",
                with_wf_link=True,
            )
    elif args.prepare:
        _prepare(token=args.token or get_best_robot_token())
    else:
        parser.print_help()
        sys.exit(2)


if __name__ == "__main__":
    main()
