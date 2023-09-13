import argparse
import logging


from get_robot_token import get_best_robot_token
import github
from typing import List, Any, TypeVar
from github_helper import GitHub

T = TypeVar("T")


def parse_args():
    parser = argparse.ArgumentParser("Create cherry-pick and backport PRs")
    parser.add_argument("--token", help="github token, if not set, used from smm")
    parser.add_argument(
        "--repo", default="ClickHouse/ClickHouse", help="repo owner/name"
    )
    parser.add_argument("--dry-run", action="store_true", help="do not create anything")
    parser.add_argument(
        "--release-after-days",
        default=3,
        help="do automatic release on the latest green commit after the latest release if the newest release is older than the specified days",
    )
    # TODO(antaljanosbenjamin): handle this
    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )

    return parser.parse_args()


def paginated_list_to_list(
    paginated_list: github.PaginatedList.PaginatedList[T],
) -> List[T]:
    return [item for item in paginated_list]


READY_FOR_RELEASE_CHECK_NAME = "Ready for release"


def main():
    args = parse_args()
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    token = args.token or get_best_robot_token()
    gh = GitHub(token)

    prs = gh.get_release_pulls(args.repo)

    branch_names = [pr.head.ref for pr in prs]

    print(branch_names)

    repo = gh.get_repo(args.repo)

    # In general there is no guarantee on which order the refs/commits are returned from the API, so we have to order them.
    for pr in prs:
        refs = paginated_list_to_list(
            repo.get_git_matching_refs(f"tags/v{pr.head.ref}")
        )
        refs.sort(key=lambda ref: ref.ref)

        latest_release_tag_ref = refs[-1]

        print(latest_release_tag_ref)

        latest_release_tag = repo.get_git_tag(latest_release_tag_ref.object.sha)

        unreleased_commits = paginated_list_to_list(
            repo.get_commits(sha=pr.head.ref, since=latest_release_tag.tagger.date)
        )
        unreleased_commits.sort(
            key=lambda commit: commit.commit.committer.date, reverse=True
        )

        released = False
        for commit in unreleased_commits:
            logging.info("Checking statuses of commit %s", commit.sha)
            for status in commit.get_statuses():
                if status.context != READY_FOR_RELEASE_CHECK_NAME:
                    continue
                if status.state != "success":
                    logging.info("Commit is not ready for release")
                    break

                logging.info("Commit is ready for release, let's release!")

                # TODO(antaljanosbenjamin: do the release here)
                released = True
                break

            if released:
                break


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
