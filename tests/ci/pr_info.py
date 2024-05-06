#!/usr/bin/env python3
import json
import logging
import os
from typing import Dict, List, Set, Union, Literal

from unidiff import PatchSet  # type: ignore

from build_download_helper import get_gh_api
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    GITHUB_RUN_URL,
    GITHUB_EVENT_PATH,
)

FORCE_TESTS_LABEL = "force tests"
SKIP_MERGEABLE_CHECK_LABEL = "skip mergeable check"
NeedsDataType = Dict[str, Dict[str, Union[str, Dict[str, str]]]]

DIFF_IN_DOCUMENTATION_EXT = [
    ".html",
    ".md",
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
        print("Cannot find PR with required ref", ref, "returning first one")
        first_pr = our_prs[0]
        return first_pr
    except Exception as ex:
        print("Cannot fetch PR info from commit", ex)
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
        self.body = ""
        self.diff_urls = []  # type: List[str]
        # release_pr and merged_pr are used for docker images additional cache
        self.release_pr = 0
        self.merged_pr = 0
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

            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL

            self.repo_full_name = GITHUB_REPOSITORY
            self.commit_html_url = f"{repo_prefix}/commit/{self.sha}"
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
            }  # type: Set[str]

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

        elif "commits" in github_event:
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
            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL
            self.commit_html_url = f"{repo_prefix}/commit/{self.sha}"
            self.repo_full_name = GITHUB_REPOSITORY
            if pull_request is None or pull_request["state"] == "closed":
                # it's merged PR to master
                self.number = 0
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
            print("event.json does not match pull_request or push:")
            print(json.dumps(github_event, sort_keys=True, indent=4))
            self.sha = os.getenv(
                "GITHUB_SHA", "0000000000000000000000000000000000000000"
            )
            self.number = 0
            self.labels = set()
            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL
            self.commit_html_url = f"{repo_prefix}/commit/{self.sha}"
            self.repo_full_name = GITHUB_REPOSITORY
            self.pr_html_url = f"{repo_prefix}/commits/{ref}"
            self.base_ref = ref
            self.base_name = self.repo_full_name
            self.head_ref = ref
            self.head_name = self.repo_full_name

        if need_changed_files:
            self.fetch_changed_files()

    def compare_pr_url(self, pr_object: dict) -> str:
        return self.compare_url(pr_object["base"]["label"], pr_object["head"]["label"])

    @staticmethod
    def compare_url(first: str, second: str) -> str:
        return (
            "https://api.github.com/repos/"
            f"{GITHUB_REPOSITORY}/compare/{first}...{second}"
        )

    def fetch_changed_files(self):
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
        # If the list wasn't built yet the best we can do is to
        # assume that there were changes.
        if self.changed_files is None or not self.changed_files:
            return True

        for f in self.changed_files:
            _, ext = os.path.splitext(f)
            path_in_docs = f.startswith("docs/")
            if (
                ext in DIFF_IN_DOCUMENTATION_EXT and path_in_docs
            ) or "docker/docs" in f:
                return True
        return False

    def has_changes_in_submodules(self):
        if self.changed_files is None or not self.changed_files:
            return True

        for f in self.changed_files:
            if "contrib/" in f:
                return True
        return False

    def can_skip_builds_and_use_version_from_master(self):
        if FORCE_TESTS_LABEL in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        return not any(
            f.startswith("programs")
            or f.startswith("src")
            or f.startswith("base")
            or f.startswith("cmake")
            or f.startswith("rust")
            or f == "CMakeLists.txt"
            or f == "tests/ci/build_check.py"
            for f in self.changed_files
        )

    def can_skip_integration_tests(self, versions: List[str]) -> bool:
        if FORCE_TESTS_LABEL in self.labels:
            return False

        # If docker image(s) relevant to integration tests are updated
        if any(self.sha in version for version in versions):
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        if not self.can_skip_builds_and_use_version_from_master():
            return False

        # Integration tests can be skipped if integration tests are not changed
        return not any(
            f.startswith("tests/integration/")
            or f == "tests/ci/integration_test_check.py"
            for f in self.changed_files
        )

    def can_skip_functional_tests(
        self, version: str, test_type: Literal["stateless", "stateful"]
    ) -> bool:
        if FORCE_TESTS_LABEL in self.labels:
            return False

        # If docker image(s) relevant to functional tests are updated
        if self.sha in version:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        if not self.can_skip_builds_and_use_version_from_master():
            return False

        # Functional tests can be skipped if queries tests are not changed
        if test_type == "stateless":
            return not any(
                f.startswith("tests/queries/0_stateless")
                or f == "tests/ci/functional_test_check.py"
                for f in self.changed_files
            )
        else:  # stateful
            return not any(
                f.startswith("tests/queries/1_stateful")
                or f == "tests/ci/functional_test_check.py"
                for f in self.changed_files
            )


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
