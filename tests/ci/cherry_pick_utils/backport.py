# -*- coding: utf-8 -*-

import argparse
import logging
import os
import re
import sys

sys.path.append(os.path.dirname(__file__))

from cherrypick import CherryPick
from query import Query as RemoteRepo
from local import Repository as LocalRepo


class Backport:
    def __init__(self, token, owner, name, team):
        self._gh = RemoteRepo(
            token, owner=owner, name=name, team=team, max_page_size=60, min_page_size=7
        )
        self._token = token
        self.default_branch_name = self._gh.default_branch
        self.ssh_url = self._gh.ssh_url

    def getPullRequests(self, from_commit):
        return self._gh.get_pull_requests(from_commit)

    def getBranchesWithRelease(self):
        branches = set()
        for pull_request in self._gh.find_pull_requests("release"):
            branches.add(pull_request["headRefName"])
        return branches

    def execute(self, repo, upstream, until_commit, run_cherrypick):
        repo = LocalRepo(repo, upstream, self.default_branch_name)
        all_branches = repo.get_release_branches()  # [(branch_name, base_commit)]

        release_branches = self.getBranchesWithRelease()

        branches = []
        # iterate over all branches to preserve their precedence.
        for branch in all_branches:
            if branch[0] in release_branches:
                branches.append(branch)

        if not branches:
            logging.info("No release branches found!")
            return

        logging.info(
            "Found release branches: %s", ", ".join([br[0] for br in branches])
        )

        if not until_commit:
            until_commit = branches[0][1]
        pull_requests = self.getPullRequests(until_commit)

        backport_map = {}
        pr_map = {pr["number"]: pr for pr in pull_requests}

        RE_MUST_BACKPORT = re.compile(r"^v(\d+\.\d+)-must-backport$")
        RE_NO_BACKPORT = re.compile(r"^v(\d+\.\d+)-no-backport$")
        RE_BACKPORTED = re.compile(r"^v(\d+\.\d+)-backported$")

        # pull-requests are sorted by ancestry from the most recent.
        for pr in pull_requests:
            while repo.comparator(branches[-1][1]) >= repo.comparator(
                pr["mergeCommit"]["oid"]
            ):
                logging.info(
                    "PR #%s is already inside %s. Dropping this branch for further PRs",
                    pr["number"],
                    branches[-1][0],
                )
                branches.pop()

            logging.info("Processing PR #%s", pr["number"])

            assert len(branches) != 0

            branch_set = {branch[0] for branch in branches}

            # First pass. Find all must-backports
            for label in pr["labels"]["nodes"]:
                if label["name"] == "pr-must-backport":
                    backport_map[pr["number"]] = branch_set.copy()
                    continue
                matched = RE_MUST_BACKPORT.match(label["name"])
                if matched:
                    if pr["number"] not in backport_map:
                        backport_map[pr["number"]] = set()
                    backport_map[pr["number"]].add(matched.group(1))

            # Second pass. Find all no-backports
            for label in pr["labels"]["nodes"]:
                if label["name"] == "pr-no-backport" and pr["number"] in backport_map:
                    del backport_map[pr["number"]]
                    break
                matched_no_backport = RE_NO_BACKPORT.match(label["name"])
                matched_backported = RE_BACKPORTED.match(label["name"])
                if (
                    matched_no_backport
                    and pr["number"] in backport_map
                    and matched_no_backport.group(1) in backport_map[pr["number"]]
                ):
                    backport_map[pr["number"]].remove(matched_no_backport.group(1))
                    logging.info(
                        "\tskipping %s because of forced no-backport",
                        matched_no_backport.group(1),
                    )
                elif (
                    matched_backported
                    and pr["number"] in backport_map
                    and matched_backported.group(1) in backport_map[pr["number"]]
                ):
                    backport_map[pr["number"]].remove(matched_backported.group(1))
                    logging.info(
                        "\tskipping %s because it's already backported manually",
                        matched_backported.group(1),
                    )

        for pr, branches in list(backport_map.items()):
            statuses = []
            for branch in branches:
                branch_status = run_cherrypick(pr_map[pr], branch)
                statuses.append(f"{branch}, and the status is: {branch_status}")
            logging.info(
                "PR #%s needs to be backported to:\n\t%s", pr, "\n\t".join(statuses)
            )

        # print API costs
        logging.info("\nGitHub API total costs for backporting per query:")
        for name, value in list(self._gh.api_costs.items()):
            logging.info("%s : %s", name, value)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--token", type=str, required=True, help="token for Github access"
    )
    parser.add_argument(
        "--repo",
        type=str,
        required=True,
        help="path to full repository",
        metavar="PATH",
    )
    parser.add_argument(
        "--til", type=str, help="check PRs from HEAD til this commit", metavar="COMMIT"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="do not create or merge any PRs",
        default=False,
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="more verbose output",
        default=False,
    )
    parser.add_argument(
        "--upstream",
        "-u",
        type=str,
        help="remote name of upstream in repository",
        default="origin",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            format="%(message)s", stream=sys.stdout, level=logging.DEBUG
        )
    else:
        logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)

    cherry_pick = CherryPick(
        args.token, "ClickHouse", "ClickHouse", "core", 1, "master"
    )

    def cherrypick_run(pr_data, branch):
        cherry_pick.update_pr_branch(pr_data, branch)
        return cherry_pick.execute(args.repo, args.dry_run)

    bp = Backport(args.token, "ClickHouse", "ClickHouse", "core")
    bp.execute(args.repo, args.upstream, args.til, cherrypick_run)
