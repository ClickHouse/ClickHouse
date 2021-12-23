#!/usr/bin/env python3
import json
import os
import urllib

import requests
from unidiff import PatchSet


DIFF_IN_DOCUMENTATION_EXT = [".html", ".md", ".yml", ".txt", ".css", ".js", ".xml", ".ico", ".conf", ".svg", ".png", ".jpg", ".py", ".sh", ".json"]

def get_pr_for_commit(sha, ref):
    try_get_pr_url = f"https://api.github.com/repos/{os.getenv('GITHUB_REPOSITORY', 'ClickHouse/ClickHouse')}/commits/{sha}/pulls"
    try:
        response = requests.get(try_get_pr_url)
        response.raise_for_status()
        data = response.json()
        if len(data) > 1:
            print("Got more than one pr for commit", sha)
        for pr in data:
            # refs for pushes looks like refs/head/XX
            # refs for RPs looks like XX
            if pr['head']['ref'] in ref:
                return pr
        print ("Cannot find PR with required ref", ref, "returning first one")
        first_pr = data[0]
        return first_pr
    except Exception as ex:
        print("Cannot fetch PR info from commit", ex)
    return None


def get_event():
    with open(os.getenv('GITHUB_EVENT_PATH'), 'r', encoding='utf-8') as ef:
        return json.load(ef)


class PRInfo:
    def __init__(self, github_event, need_orgs=False, need_changed_files=False):
        if 'pull_request' in github_event: # pull request and other similar events
            self.number = github_event['number']
            if 'after' in github_event:
                self.sha = github_event['after']
            else:
                self.sha = github_event['pull_request']['head']['sha']

            repo_prefix = f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/{os.getenv('GITHUB_REPOSITORY', 'ClickHouse/ClickHouse')}"
            self.task_url = f"{repo_prefix}/actions/runs/{os.getenv('GITHUB_RUN_ID')}"

            self.repo_full_name = os.getenv('GITHUB_REPOSITORY', 'ClickHouse/ClickHouse')
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.pr_html_url = f"{repo_prefix}/pull/{self.number}"

            self.base_ref = github_event['pull_request']['base']['ref']
            self.base_name = github_event['pull_request']['base']['repo']['full_name']
            self.head_ref = github_event['pull_request']['head']['ref']
            self.head_name = github_event['pull_request']['head']['repo']['full_name']

            self.labels = { l['name'] for l in github_event['pull_request']['labels'] }
            self.user_login = github_event['pull_request']['user']['login']
            self.user_orgs = set([])
            if need_orgs:
                user_orgs_response = requests.get(github_event['pull_request']['user']['organizations_url'])
                if user_orgs_response.ok:
                    response_json = user_orgs_response.json()
                    self.user_orgs = set(org['id'] for org in response_json)

            self.changed_files = set([])
            if need_changed_files:
                diff_url = github_event['pull_request']['diff_url']
                diff = urllib.request.urlopen(diff_url)
                diff_object = PatchSet(diff, diff.headers.get_charsets()[0])
                self.changed_files = { f.path for f in diff_object }

        elif 'commits' in github_event:
            self.sha = github_event['after']
            pull_request = get_pr_for_commit(self.sha, github_event['ref'])
            repo_prefix = f"{os.getenv('GITHUB_SERVER_URL', 'https://github.com')}/{os.getenv('GITHUB_REPOSITORY', 'ClickHouse/ClickHouse')}"
            self.task_url = f"{repo_prefix}/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
            self.commit_html_url = f"{repo_prefix}/commits/{self.sha}"
            self.repo_full_name = os.getenv('GITHUB_REPOSITORY', 'ClickHouse/ClickHouse')
            if pull_request is None or pull_request['state'] == 'closed': # it's merged PR to master
                self.number = 0
                self.labels = {}
                self.pr_html_url = f"{repo_prefix}/commits/master"
                self.base_ref = "master"
                self.base_name = self.repo_full_name
                self.head_ref = "master"
                self.head_name = self.repo_full_name
            else:
                self.number = pull_request['number']
                self.labels = { l['name'] for l in pull_request['labels'] }
                self.base_ref = pull_request['base']['ref']
                self.base_name = pull_request['base']['repo']['full_name']
                self.head_ref = pull_request['head']['ref']
                self.head_name = pull_request['head']['repo']['full_name']
                self.pr_html_url = pull_request['html_url']

            if need_changed_files:
                if self.number == 0:
                    commit_before = github_event['before']
                    response = requests.get(f"https://api.github.com/repos/{os.getenv('GITHUB_REPOSITORY')}/compare/{commit_before}...{self.sha}")
                    response.raise_for_status()
                    diff = response.json()

                    if 'files' in diff:
                        self.changed_files = [f['filename'] for f in diff['files']]
                    else:
                        self.changed_files = set([])
                else:
                    if 'pr-backport' in self.labels:
                        diff_url = f"https://github.com/{os.getenv('GITHUB_REPOSITORY')}/compare/master...{self.head_ref}.diff"
                    else:
                        diff_url = pull_request['diff_url']

                    diff = urllib.request.urlopen(diff_url)
                    diff_object = PatchSet(diff, diff.headers.get_charsets()[0])
                    self.changed_files = { f.path for f in diff_object }
            else:
                self.changed_files = set([])
        else:
            raise Exception("Cannot detect type of event")


    def get_dict(self):
        return {
            'sha': self.sha,
            'number': self.number,
            'labels': self.labels,
            'user_login': self.user_login,
            'user_orgs': self.user_orgs,
        }

    def has_changes_in_documentation(self):
        # If the list wasn't built yet the best we can do is to
        # assume that there were changes.
        if self.changed_files is None or not self.changed_files:
            return True

        for f in self.changed_files:
            _, ext = os.path.splitext(f)
            path_in_docs = 'docs' in f
            path_in_website = 'website' in f
            if (ext in DIFF_IN_DOCUMENTATION_EXT and (path_in_docs or path_in_website)) or 'docker/docs' in f:
                return True
        return False

    def can_skip_builds_and_use_version_from_master(self):
        if 'force tests' in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            if (not f.startswith('tests/queries')
                or not f.startswith('tests/integration')
                or not f.startswith('tests/performance')):
                return False

        return True

    def can_skip_integration_tests(self):
        if 'force tests' in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            if not f.startswith('tests/queries') or not f.startswith('tests/performance'):
                return False

        return True

    def can_skip_functional_tests(self):
        if 'force tests' in self.labels:
            return False

        if self.changed_files is None or not self.changed_files:
            return False

        for f in self.changed_files:
            if not f.startswith('tests/integration') or not f.startswith('tests/performance'):
                return False

        return True


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
