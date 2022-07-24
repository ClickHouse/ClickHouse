#!/usr/bin/env python3
import json
import os
from typing import Set

from unidiff import PatchSet  # type: ignore

from build_download_helper import get_with_retries
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    GITHUB_RUN_URL,
    GITHUB_EVENT_PATH,
)

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
        response = get_with_retries(try_get_pr_url, sleep=RETRY_SLEEP)
        data = response.json()
        if len(data) > 1:
            print("Got more than one pr for commit", sha)
        for pr in data:
            # refs for pushes looks like refs/head/XX
            # refs for RPs looks like XX
            if pr["head"]["ref"] in ref:
                return pr
        print("Cannot find PR with required ref", ref, "returning first one")
        first_pr = data[0]
        return first_pr
    except Exception as ex:
        print("Cannot fetch PR info from commit", ex)
    return None


class PRInfo:
    default_event = {
        "commits": 1,
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
        self.diff_urls = []
        self.release_pr = ""
        ref = github_event.get("ref", "refs/head/master")
        if ref and ref.startswith("refs/heads/"):
            ref = ref[11:]

        # workflow completed event, used for PRs only
        if "action" in github_event and github_event["action"] == "completed":
            self.sha = github_event["workflow_run"]["head_sha"]
            prs_for_sha = get_with_retries(
                f"https://api.github.com/repos/{GITHUB_REPOSITORY}/commits/{self.sha}"
                "/pulls",
                sleep=RETRY_SLEEP,
            ).json()
            if len(prs_for_sha) != 0:
                github_event["pull_request"] = prs_for_sha[0]

        if "pull_request" in github_event:  # pull request and other similar events
            self.number = github_event["pull_request"]["number"]
            if pr_event_from_api:
                response = get_with_retries(
                    f"https://api.github.com/repos/{GITHUB_REPOSITORY}"
                    f"/pulls/{self.number}",
                    sleep=RETRY_SLEEP,
                )
                github_event["pull_request"] = response.json()

            if "after" in github_event:
                self.sha = github_event["after"]
            else:
                self.sha = github_event["pull_request"]["head"]["sha"]

            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL

            self.repo_full_name = GITHUB_REPOSITORY
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.pr_html_url = f"{repo_prefix}/pull/{self.number}"

            self.base_ref = github_event["pull_request"]["base"]["ref"]
            self.base_name = github_event["pull_request"]["base"]["repo"]["full_name"]
            self.head_ref = github_event["pull_request"]["head"]["ref"]
            self.head_name = github_event["pull_request"]["head"]["repo"]["full_name"]
            self.body = github_event["pull_request"]["body"]
            self.labels = {
                label["name"] for label in github_event["pull_request"]["labels"]
            }

            self.user_login = github_event["pull_request"]["user"]["login"]
            self.user_orgs = set([])
            if need_orgs:
                user_orgs_response = get_with_retries(
                    github_event["pull_request"]["user"]["organizations_url"],
                    sleep=RETRY_SLEEP,
                )
                if user_orgs_response.ok:
                    response_json = user_orgs_response.json()
                    self.user_orgs = set(org["id"] for org in response_json)

            self.diff_urls.append(github_event["pull_request"]["diff_url"])
        elif "commits" in github_event:
            self.sha = github_event["after"]
            pull_request = get_pr_for_commit(self.sha, github_event["ref"])
            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.repo_full_name = GITHUB_REPOSITORY
            if pull_request is None or pull_request["state"] == "closed":
                # it's merged PR to master
                self.number = 0
                self.labels = {}
                self.pr_html_url = f"{repo_prefix}/commits/{ref}"
                self.base_ref = ref
                self.base_name = self.repo_full_name
                self.head_ref = ref
                self.head_name = self.repo_full_name
                self.diff_urls.append(
                    f"https://api.github.com/repos/{GITHUB_REPOSITORY}/"
                    f"compare/{github_event['before']}...{self.sha}"
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
                        f"https://github.com/{GITHUB_REPOSITORY}/"
                        f"compare/master...{self.head_ref}.diff"
                    )
                    self.diff_urls.append(
                        f"https://github.com/{GITHUB_REPOSITORY}/"
                        f"compare/{self.head_ref}...master.diff"
                    )
                    # Get release PR number.
                    self.release_pr = get_pr_for_commit(self.base_ref, self.base_ref)[
                        "number"
                    ]
                else:
                    self.diff_urls.append(pull_request["diff_url"])
                if "release" in self.labels:
                    # For release PRs we must get not only files changed in the PR
                    # itself, but as well files changed since we branched out
                    self.diff_urls.append(
                        f"https://github.com/{GITHUB_REPOSITORY}/"
                        f"compare/{self.head_ref}...master.diff"
                    )
        else:
            print(json.dumps(github_event, sort_keys=True, indent=4))
            self.sha = os.getenv("GITHUB_SHA")
            self.number = 0
            self.labels = {}
            repo_prefix = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
            self.task_url = GITHUB_RUN_URL
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.repo_full_name = GITHUB_REPOSITORY
            self.pr_html_url = f"{repo_prefix}/commits/{ref}"
            self.base_ref = ref
            self.base_name = self.repo_full_name
            self.head_ref = ref
            self.head_name = self.repo_full_name

        if need_changed_files:
            self.fetch_changed_files()

    def fetch_changed_files(self):
        if not getattr(self, "diff_urls", False):
            raise TypeError("The event does not have diff URLs")

        for diff_url in self.diff_urls:
            response = get_with_retries(
                diff_url,
                sleep=RETRY_SLEEP,
            )
            response.raise_for_status()
            if "commits" in self.event and self.number == 0:
                diff = response.json()

                if "files" in diff:
                    self.changed_files = {f["filename"] for f in diff["files"]}
            else:
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

    def has_changes_in_documentation(self):
        # If the list wasn't built yet the best we can do is to
        # assume that there were changes.
        if self.changed_files is None or not self.changed_files:
            return True

        for f in self.changed_files:
            _, ext = os.path.splitext(f)
            path_in_docs = "docs" in f
            path_in_website = "website" in f
            if (
                ext in DIFF_IN_DOCUMENTATION_EXT and (path_in_docs or path_in_website)
            ) or "docker/docs" in f:
                return True
        return False

    def can_skip_builds_and_use_version_from_master(self):
        # TODO: See a broken loop
        if "force tests" in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            # TODO: this logic is broken, should be fixed before using
            if (
                not f.startswith("tests/queries")
                or not f.startswith("tests/integration")
                or not f.startswith("tests/performance")
            ):
                return False

        return True

    def can_skip_integration_tests(self):
        # TODO: See a broken loop
        if "force tests" in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            # TODO: this logic is broken, should be fixed before using
            if not f.startswith("tests/queries") or not f.startswith(
                "tests/performance"
            ):
                return False

        return True

    def can_skip_functional_tests(self):
        # TODO: See a broken loop
        if "force tests" in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            # TODO: this logic is broken, should be fixed before using
            if not f.startswith("tests/integration") or not f.startswith(
                "tests/performance"
            ):
                return False

        return True


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
