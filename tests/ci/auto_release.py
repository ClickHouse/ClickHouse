import argparse
from datetime import timedelta, datetime
import logging
import os
from commit_status_helper import get_commit_filtered_statuses
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from release import Release, Repo as ReleaseRepo, RELEASE_READY_STATUS
from report import SUCCESS
from ssh import SSHKey

LOGGER_NAME = __name__
HELPER_LOGGERS = ["github_helper", LOGGER_NAME]
logger = logging.getLogger(LOGGER_NAME)


def parse_args():
    parser = argparse.ArgumentParser(
        "Checks if enough days elapsed since the last release on each release "
        "branches and do a release in case for green builds."
    )
    parser.add_argument("--token", help="GitHub token, if not set, used from smm")
    parser.add_argument(
        "--repo", default="ClickHouse/ClickHouse", help="Repo owner/name"
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not create anything")
    parser.add_argument(
        "--release-after-days",
        type=int,
        default=3,
        help="Do automatic release on the latest green commit after the latest "
        "release if the newest release is older than the specified days",
    )
    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="Add debug logging for this script and github_helper",
    )
    parser.add_argument(
        "--remote-protocol",
        "-p",
        default="ssh",
        choices=ReleaseRepo.VALID,
        help="repo protocol for git commands remote, 'origin' is a special case and "
        "uses 'origin' as a remote",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.debug_helpers:
        for logger_name in HELPER_LOGGERS:
            logging.getLogger(logger_name).setLevel(logging.DEBUG)

    token = args.token or get_best_robot_token()
    days_as_timedelta = timedelta(days=args.release_after_days)
    now = datetime.now()

    gh = GitHub(token)
    prs = gh.get_release_pulls(args.repo)
    branch_names = [pr.head.ref for pr in prs]

    logger.info("Found release branches: %s\n ", " \n".join(branch_names))
    repo = gh.get_repo(args.repo)

    # In general there is no guarantee on which order the refs/commits are
    # returned from the API, so we have to order them.
    for pr in prs:
        logger.info("Checking PR %s", pr.head.ref)

        refs = list(repo.get_git_matching_refs(f"tags/v{pr.head.ref}"))
        refs.sort(key=lambda ref: ref.ref)

        latest_release_tag_ref = refs[-1]
        latest_release_tag = repo.get_git_tag(latest_release_tag_ref.object.sha)
        logger.info("That last release was done at %s", latest_release_tag.tagger.date)

        if latest_release_tag.tagger.date + days_as_timedelta > now:
            logger.info(
                "Not enough days since the last release %s,"
                " no automatic release can be done",
                latest_release_tag.tag,
            )
            continue

        unreleased_commits = list(
            repo.get_commits(sha=pr.head.ref, since=latest_release_tag.tagger.date)
        )
        unreleased_commits.sort(
            key=lambda commit: commit.commit.committer.date, reverse=True
        )

        for commit in unreleased_commits:
            logger.info("Checking statuses of commit %s", commit.sha)
            statuses = get_commit_filtered_statuses(commit)
            all_success = all(st.state == SUCCESS for st in statuses)
            passed_ready_for_release_check = any(
                st.context == RELEASE_READY_STATUS and st.state == SUCCESS
                for st in statuses
            )
            if not (all_success and passed_ready_for_release_check):
                logger.info("Commit is not green, thus not suitable for release")
                continue

            logger.info("Commit is ready for release, let's release!")

            release = Release(
                ReleaseRepo(args.repo, args.remote_protocol),
                commit.sha,
                "patch",
                args.dry_run,
                True,
            )
            try:
                release.do(True, True, True)
            except:
                if release.has_rollback:
                    logging.error(
                        "!!The release process finished with error, read the output carefully!!"
                    )
                    logging.error(
                        "Probably, rollback finished with error. "
                        "If you don't see any of the following commands in the output, "
                        "execute them manually:"
                    )
                    release.log_rollback()
                raise
            logging.info("New release is done!")
            break


if __name__ == "__main__":
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            main()
    else:
        main()
