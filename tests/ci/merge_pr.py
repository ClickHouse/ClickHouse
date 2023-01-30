#!/usr/bin/env python

"""Script to check if PR is mergeable and merge it"""

import argparse
import logging

from datetime import datetime
from os import getenv
from pprint import pformat
from typing import Dict, List

from github.PullRequestReview import PullRequestReview

from commit_status_helper import get_commit_filtered_statuses
from get_robot_token import get_best_robot_token
from github_helper import GitHub, NamedUser, PullRequest
from pr_info import PRInfo


# The team name for accepted approvals
TEAM_NAME = getenv("GITHUB_TEAM_NAME", "core")


class Reviews:
    STATES = ["CHANGES_REQUESTED", "APPROVED"]

    def __init__(self, pr: PullRequest):
        """The reviews are proceed in the next logic:
        - if review for an author does not exist, set it
        - the review status can be changed from CHANGES_REQUESTED and APPROVED
            only to either one
        """
        logging.info("Checking the PR for approvals")
        self.pr = pr
        reviews = pr.get_reviews()
        # self.reviews is a dict of latest CHANGES_REQUESTED or APPROVED review
        # per user
        # NamedUsed has proper __eq__ and __hash__, so it's safe to use it
        self.reviews = {}  # type: Dict[NamedUser, PullRequestReview]
        for r in reviews:
            user = r.user

            if not self.reviews.get(user):
                self.reviews[user] = r
                continue

            if r.submitted_at < self.reviews[user].submitted_at:
                self.reviews[user] = r

    def is_approved(self, team: List[NamedUser]) -> bool:
        """Checks if the PR is approved, and no changes made after the last approval"""
        if not self.reviews:
            logging.info("There aren't reviews for PR #%s", self.pr.number)
            return False

        filtered_reviews = {
            user: review
            for user, review in self.reviews.items()
            if review.state in self.STATES and user in team
        }

        # We consider reviews only from the given list of users
        changes_requested = {
            user: review
            for user, review in filtered_reviews.items()
            if review.state == "CHANGES_REQUESTED"
        }

        if changes_requested:
            logging.info(
                "The following users requested changes for the PR: %s",
                ", ".join(user.login for user in changes_requested.keys()),
            )
            return False

        approved = {
            user: review
            for user, review in filtered_reviews.items()
            if review.state == "APPROVED"
        }

        if approved:
            logging.info(
                "The following users from %s team approved the PR: %s",
                TEAM_NAME,
                ", ".join(user.login for user in approved.keys()),
            )
            # The only reliable place to get the 100% accurate last_modified
            # info is when the commit was pushed to GitHub. The info is
            # available as a header 'last-modified' of /{org}/{repo}/commits/{sha}.
            # Unfortunately, it's formatted as 'Wed, 04 Jan 2023 11:05:13 GMT'

            commit = self.pr.head.repo.get_commit(self.pr.head.sha)
            if commit.stats.last_modified is None:
                logging.warning(
                    "Unable to get info about the commit %s", self.pr.head.sha
                )
                return False

            last_changed = datetime.strptime(
                commit.stats.last_modified, "%a, %d %b %Y %H:%M:%S GMT"
            )

            approved_at = max(review.submitted_at for review in approved.values())
            if approved_at == datetime.fromtimestamp(0):
                logging.info(
                    "Unable to get `datetime.fromtimestamp(0)`, "
                    "here's debug info about reviews: %s",
                    "\n".join(pformat(review) for review in self.reviews.values()),
                )
            else:
                logging.info(
                    "The PR is approved at %s",
                    approved_at.isoformat(),
                )

            if approved_at < last_changed:
                logging.info(
                    "There are changes after approve at %s",
                    approved_at.isoformat(),
                )
                return False
            return True

        logging.info("The PR #%s is not approved", self.pr.number)
        return False


def parse_args() -> argparse.Namespace:
    pr_info = PRInfo()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to merge the given PR. Additional checks for approved "
        "status and green commit statuses could be done",
    )
    parser.add_argument(
        "--check-approved",
        action="store_true",
        help="if set, checks that the PR is approved and no changes required",
    )
    parser.add_argument("--check-green", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-check-green",
        dest="check_green",
        action="store_false",
        default=argparse.SUPPRESS,
        help="(dangerous) if set, skip check commit to having all green statuses",
    )
    parser.add_argument(
        "--repo",
        default=pr_info.repo_full_name,
        help="PR number to check",
    )
    parser.add_argument(
        "--pr",
        type=int,
        default=pr_info.number,
        help="PR number to check",
    )
    parser.add_argument(
        "--token",
        type=str,
        default="",
        help="a token to use for GitHub API requests, will be received from SSM "
        "if empty",
    )
    args = parser.parse_args()
    args.pr_info = pr_info
    return args


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    args = parse_args()
    logging.info("Going to process PR #%s in repo %s", args.pr, args.repo)
    token = args.token or get_best_robot_token()
    gh = GitHub(token, per_page=100)
    repo = gh.get_repo(args.repo)
    # An ugly and not nice fix to patch the wrong organization URL,
    # see https://github.com/PyGithub/PyGithub/issues/2395#issuecomment-1378629710
    # pylint: disable=protected-access
    repo.organization._url.value = repo.organization.url.replace(  # type: ignore
        "/users/", "/orgs/", 1
    )
    # pylint: enable=protected-access
    pr = repo.get_pull(args.pr)
    if pr.is_merged():
        logging.info("The PR #%s is already merged", pr.number)
        return

    not_ready_to_merge = pr.draft or "WIP" in pr.title
    if not_ready_to_merge:
        logging.info("The PR #%s is not ready for merge, stopping", pr.number)
        return

    if args.check_green:
        logging.info("Checking that all PR's statuses are green")
        commit = repo.get_commit(pr.head.sha)
        failed_statuses = [
            status.context
            for status in get_commit_filtered_statuses(commit)
            if status.state != "success"
        ]
        if failed_statuses:
            logging.warning(
                "Some statuses aren't success:\n  %s", ",\n  ".join(failed_statuses)
            )
            return

    if args.check_approved:
        reviews = Reviews(pr)
        team = repo.organization.get_team_by_slug(TEAM_NAME)
        members = list(team.get_members())
        if not reviews.is_approved(members):
            logging.warning("We don't merge the PR")
            return

    logging.info("Merging the PR")
    pr.merge()


if __name__ == "__main__":
    main()
