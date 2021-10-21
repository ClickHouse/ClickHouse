#!/usr/bin/env python3
import requests
import json
import os
import subprocess
import urllib
from unidiff import PatchSet


class PRInfo:
    def __init__(self, github_event, need_orgs=False, need_changed_files=False):
        self.number = github_event['number']
        if 'after' in github_event:
            self.sha = github_event['after']
        else:
            self.sha = github_event['pull_request']['head']['sha']

        self.labels = set([l['name'] for l in github_event['pull_request']['labels']])
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
            diff = urllib.request.urlopen(github_event['pull_request']['diff_url'])
            diff_object = PatchSet(diff, diff.headers.get_charsets()[0])
            self.changed_files = set([f.path for f in diff_object])

    def get_dict(self):
        return {
            'sha': self.sha,
            'number': self.number,
            'labels': self.labels,
            'user_login': self.user_login,
            'user_orgs': self.user_orgs,
        }


class FakePRInfo:
    def __init__(self):
        self.number = 11111
        self.sha = "xxxxxxxxxxxxxxxxxx"
