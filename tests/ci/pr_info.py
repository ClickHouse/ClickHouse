#!/usr/bin/env python3
import json
import logging
import os
import re
from typing import Dict, List, Set, Union
from urllib.parse import quote

from unidiff import PatchSet  # type: ignore

from build_download_helper import get_gh_api
from env_helper import (
    GITHUB_EVENT_PATH,
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    GITHUB_SERVER_URL,
)

SKIP_MERGEABLE_CHECK_LABEL = "skip mergeable check"
NeedsDataType = Dict[str, Dict[str, Union[str, Dict[str, str]]]]

DIFF_IN_DOCUMENTATION_EXT = [
    ".html",
    ".md",
    ".mdx",
    ".yml",
    ".txt",
    ".css",
    ".js",
    ".xml",
    ".ico",
    ".conf",
    ".svg",
    ".png",
    ".jpg",
    ".py",
    ".sh",
    ".json",
]
RETRY_SLEEP = 0


class EventType:
    UNKNOWN = "unknown"
    PUSH = "commits"
    PULL_REQUEST = "pull_request"
    SCHEDULE = "schedule"
    DISPATCH = "dispatch"
    MERGE_QUEUE = "merge_group"


def get_pr_for_commit(sha, ref):
    if not ref:
        return None
    try_get_pr_url = (
        f"https://api.github.com/repos/{GITHUB_REPOSITORY}/commits/{sha}/pulls"
    )
    try:
        response = get_gh_api(try_get_pr_url, sleep=RETRY_SLEEP)
        data = response.json()
        our_prs = []  # type: List[Dict]
        if len(data) > 1:
            print("Got more than one pr for commit", sha)
        for pr in data:
            # We need to check if the PR is created in our repo, because
            # https://github.com/kaynewu/ClickHouse/pull/2
            # has broke our PR search once in a while
            if pr["base"]["repo"]["full_name"] != GITHUB_REPOSITORY:
                continue
            # refs for pushes looks like refs/head/XX
            # refs for RPs looks like XX
            if pr["head"]["ref"] in ref:
                return pr
            our_prs.append(pr)
        print(
            f"Cannot find PR with required ref {ref}, sha {sha} - returning first one"
        )
        first_pr = our_prs[0]
        return first_pr
    except Exception as ex:
        print(f"Cannot fetch PR info from commit {ref}, {sha}", ex)
    return None


class PRInfo:
    default_event = {
        "commits": 1,
        "head_commit": {"message": "commit_message"},
        "before": "HEAD~",
        "after": "HEAD",
        "ref": None,
    }

    def __init__(
        self,
        github_event=None,
        need_orgs=False,
        need_changed_files=False,
        pr_event_from_api=False,
    ):
        if not github_event:
            if GITHUB_EVENT_PATH:
                with open(GITHUB_EVENT_PATH, "r", encoding="utf-8") as event_file:
                    github_event = json.load(event_file)
            else:
                github_event = PRInfo.default_event.copy()
        self.event = github_event
        self.changed_files = set()  # type: Set[str]
        self.changed_files_requested = False
        self.body = ""
        self.diff_urls = []  # type: List[str]
        # release_pr and merged_pr are used for docker images additional cache
        self.release_pr = 0
        self.merged_pr = 0
        self.labels = set()

        repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
        self.task_url = GITHUB_RUN_URL
        self.repo_full_name = GITHUB_REPOSITORY

        self.event_type = EventType.UNKNOWN
        ref = github_event.get("ref", "refs/heads/master")
        if ref and ref.startswith("refs/heads/"):
            ref = ref[11:]

        # workflow completed event, used for PRs only
        if "action" in github_event and github_event["action"] == "completed":
            self.sha = github_event["workflow_run"]["head_sha"]  # type: str
            prs_for_sha = get_gh_api(
                f"https://api.github.com/repos/{GITHUB_REPOSITORY}/commits/{self.sha}"
                "/pulls",
                sleep=RETRY_SLEEP,
            ).json()
            if len(prs_for_sha) != 0:
                github_event["pull_request"] = prs_for_sha[0]

        if "pull_request" in github_event:  # pull request and other similar events
            self.event_type = EventType.PULL_REQUEST
            self.number = github_event["pull_request"]["number"]  # type: int
            if pr_event_from_api:
                try:
                    response = get_gh_api(
                        f"https://api.github.com/repos/{GITHUB_REPOSITORY}"
                        f"/pulls/{self.number}",
                        sleep=RETRY_SLEEP,
                    )
                    github_event["pull_request"] = response.json()
                except Exception as e:
                    logging.warning(
                        "Unable to get pull request event %s from API, "
                        "fallback to received event. Exception: %s",
                        self.number,
                        e,
                    )

            if "after" in github_event:
                self.sha = github_event["after"]
            else:
                self.sha = github_event["pull_request"]["head"]["sha"]

            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.pr_html_url = f"{repo_prefix}/pull/{self.number}"

            # master or backport/xx.x/xxxxx - where the PR will be merged
            self.base_ref = github_event["pull_request"]["base"]["ref"]  # type: str
            # ClickHouse/ClickHouse
            self.base_name = github_event["pull_request"]["base"]["repo"][
                "full_name"
            ]  # type: str
            # any_branch-name - the name of working branch name
            self.head_ref = github_event["pull_request"]["head"]["ref"]  # type: str
            # UserName/ClickHouse or ClickHouse/ClickHouse
            self.head_name = github_event["pull_request"]["head"]["repo"][
                "full_name"
            ]  # type: str
            self.body = github_event["pull_request"]["body"]
            self.labels = {
                label["name"] for label in github_event["pull_request"]["labels"]
            }

            self.user_login = github_event["pull_request"]["user"]["login"]  # type: str
            self.user_orgs = set()  # type: Set[str]
            if need_orgs:
                user_orgs_response = get_gh_api(
                    github_event["pull_request"]["user"]["organizations_url"],
                    sleep=RETRY_SLEEP,
                )
                if user_orgs_response.ok:
                    response_json = user_orgs_response.json()
                    self.user_orgs = set(org["id"] for org in response_json)

            self.diff_urls.append(self.compare_pr_url(github_event["pull_request"]))

        elif (
            EventType.MERGE_QUEUE in github_event
        ):  # pull request and other similar events
            self.event_type = EventType.MERGE_QUEUE
            self.number = 0
            self.sha = github_event[EventType.MERGE_QUEUE]["head_sha"]
            self.base_ref = github_event[EventType.MERGE_QUEUE]["base_ref"]
            base_sha = github_event[EventType.MERGE_QUEUE]["base_sha"]  # type: str
            # ClickHouse/ClickHouse
            self.base_name = github_event["repository"]["full_name"]
            # any_branch-name - the name of working branch name
            self.head_ref = github_event[EventType.MERGE_QUEUE]["head_ref"]
            # parse underlying pr from ["head_ref": "refs/heads/gh-readonly-queue/test-merge-queue/pr-6751-4690229995a155e771c52e95fbd446d219c069bf"]
            self.merged_pr = int(self.head_ref.split("/pr-")[-1].split("-")[0])
            # UserName/ClickHouse or ClickHouse/ClickHouse
            self.head_name = self.base_name
            self.user_login = github_event["sender"]["login"]
            self.diff_urls.append(
                github_event["repository"]["compare_url"]
                .replace("{base}", base_sha)
                .replace("{head}", self.sha)
            )
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"

        elif "commits" in github_event:
            self.event_type = EventType.PUSH
            # `head_commit` always comes with `commits`
            commit_message = github_event["head_commit"]["message"]  # type: str
            if commit_message.startswith("Merge pull request #"):
                merged_pr = commit_message.split(maxsplit=4)[3]
                try:
                    self.merged_pr = int(merged_pr[1:])
                except ValueError:
                    logging.error("Failed to convert %s to integer", merged_pr)
            self.sha = github_event["after"]
            pull_request = get_pr_for_commit(self.sha, github_event["ref"])
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"

            if pull_request is None or pull_request["state"] == "closed":
                # it's merged PR to master
                self.number = 0
                if pull_request:
                    self.merged_pr = pull_request["number"]
                self.labels = set()
                self.pr_html_url = f"{repo_prefix}/commits/{ref}"
                self.base_ref = ref
                self.base_name = self.repo_full_name
                self.head_ref = ref
                self.head_name = self.repo_full_name
                self.diff_urls.append(
                    self.compare_url(github_event["before"], self.sha)
                )
            else:
                self.number = pull_request["number"]
                self.labels = {label["name"] for label in pull_request["labels"]}

                self.base_ref = pull_request["base"]["ref"]
                self.base_name = pull_request["base"]["repo"]["full_name"]
                self.head_ref = pull_request["head"]["ref"]
                self.head_name = pull_request["head"]["repo"]["full_name"]
                self.pr_html_url = pull_request["html_url"]
                if "pr-backport" in self.labels:
                    # head1...head2 gives changes in head2 since merge base
                    # Thag's why we need {self.head_ref}...master to get
                    # files changed in upstream AND master...{self.head_ref}
                    # to get files, changed in current HEAD
                    self.diff_urls.append(
                        self.compare_url(
                            pull_request["base"]["repo"]["default_branch"],
                            pull_request["head"]["label"],
                        )
                    )
                    self.diff_urls.append(
                        self.compare_url(
                            pull_request["head"]["label"],
                            pull_request["base"]["repo"]["default_branch"],
                        )
                    )
                    # Get release PR number.
                    self.release_pr = get_pr_for_commit(self.base_ref, self.base_ref)[
                        "number"
                    ]
                else:
                    self.diff_urls.append(self.compare_pr_url(pull_request))
                if "release" in self.labels:
                    # For release PRs we must get not only files changed in the PR
                    # itself, but as well files changed since we branched out
                    self.diff_urls.append(
                        self.compare_url(
                            pull_request["head"]["label"],
                            pull_request["base"]["repo"]["default_branch"],
                        )
                    )
        else:
            if "schedule" in github_event:
                self.event_type = EventType.SCHEDULE
            else:
                # assume this is a dispatch
                self.event_type = EventType.DISPATCH
            print("event.json does not match pull_request or push:")
            print(json.dumps(github_event, sort_keys=True, indent=4))
            self.sha = os.getenv(
                "GITHUB_SHA", "0000000000000000000000000000000000000000"
            )
            self.number = 0
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.pr_html_url = f"{repo_prefix}/commits/{ref}"
            self.base_ref = ref
            self.base_name = self.repo_full_name
            self.head_ref = ref
            self.head_name = self.repo_full_name

        if need_changed_files:
            self.fetch_changed_files()

    @property
    def is_master(self) -> bool:
        return self.number == 0 and self.head_ref == "master"

    @property
    def is_release(self) -> bool:
        return self.number == 0 and bool(
            re.match(r"^2[1-9]\.[1-9][0-9]*$", self.head_ref)
        )

    @property
    def is_release_branch(self) -> bool:
        return self.number == 0

    @property
    def is_pr(self):
        return self.event_type == EventType.PULL_REQUEST

    @property
    def is_scheduled(self) -> bool:
        return self.event_type == EventType.SCHEDULE

    @property
    def is_merge_queue(self) -> bool:
        return self.event_type == EventType.MERGE_QUEUE

    @property
    def is_dispatched(self) -> bool:
        return self.event_type == EventType.DISPATCH

    def compare_pr_url(self, pr_object: dict) -> str:
        return self.compare_url(pr_object["base"]["label"], pr_object["head"]["label"])

    @staticmethod
    def compare_url(first: str, second: str) -> str:
        """the first and second are URL encoded to not fail on '#' and other symbols"""
        return (
            "https://api.github.com/repos/"
            f"{GITHUB_REPOSITORY}/compare/{quote(first)}...{quote(second)}"
        )

    def fetch_changed_files(self):
        if self.changed_files_requested:
            return

        if not getattr(self, "diff_urls", False):
            raise TypeError("The event does not have diff URLs")

        for diff_url in self.diff_urls:
            response = get_gh_api(
                diff_url,
                sleep=RETRY_SLEEP,
                headers={"Accept": "application/vnd.github.v3.diff"},
            )
            response.raise_for_status()
            diff_object = PatchSet(response.text)
            self.changed_files.update({f.path for f in diff_object})
        self.changed_files_requested = True
        print(f"Fetched info about {len(self.changed_files)} changed files")

    def get_dict(self):
        return {
            "sha": self.sha,
            "number": self.number,
            "labels": self.labels,
            "user_login": self.user_login,
            "user_orgs": self.user_orgs,
        }

    def has_changes_in_documentation(self) -> bool:
        if not self.changed_files_requested:
            self.fetch_changed_files()

        if not self.changed_files:
            return True

        for f in self.changed_files:
            _, ext = os.path.splitext(f)
            path_in_docs = f.startswith("docs/")
            if (
                ext in DIFF_IN_DOCUMENTATION_EXT and path_in_docs
            ) or "docker/docs" in f:
                return True
        return False

    def has_changes_in_documentation_only(self) -> bool:
        """
        checks if changes are docs related without other changes
        FIXME: avoid hardcoding filenames here
        """
        if not self.changed_files_requested:
            self.fetch_changed_files()

        if not self.changed_files:
            # if no changes at all return False
            return False

        for f in self.changed_files:
            _, ext = os.path.splitext(f)
            path_in_docs = f.startswith("docs/")
            if not (
                (ext in DIFF_IN_DOCUMENTATION_EXT and path_in_docs)
                or "docker/docs" in f
                or "docs_check.py" in f
                or "aspell-dict.txt" in f
                or ext == ".md"
            ):
                return False
        return True

    def has_changes_in_submodules(self):
        if not self.changed_files_requested:
            self.fetch_changed_files()

        if not self.changed_files:
            return True

        for f in self.changed_files:
            if "contrib/" in f:
                return True
        return False


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
