#!/usr/bin/env python3
import os
import urllib

import requests
from unidiff import PatchSet


DIFF_IN_DOCUMENTATION_EXT = [".html", ".md", ".yml", ".txt", ".css", ".js", ".xml", ".ico", ".conf", ".svg", ".png", ".jpg", ".py", ".sh"]

class PRInfo:
    def __init__(self, github_event, need_orgs=False, need_changed_files=False):
        if 'pull_request' in github_event: # pull request and other similar events
            self.number = github_event['number']
            if 'after' in github_event:
                self.sha = github_event['after']
            else:
                self.sha = github_event['pull_request']['head']['sha']

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
            self.number = 0
            self.sha = github_event['after']
            self.labels = {}
            if need_changed_files:
                commit_before = github_event['before']
                response = requests.get(f'https://api.github.com/repos/ClickHouse/ClickHouse/compare/{commit_before}...{self.sha}')
                response.raise_for_status()
                diff = response.json()

                if 'files' in diff:
                    self.changed_files = [f['filename'] for f in diff['files']]
                else:
                    self.changed_files = set([])
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
            if ext in DIFF_IN_DOCUMENTATION_EXT or 'Dockerfile' in f:
                return True
        return False


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
