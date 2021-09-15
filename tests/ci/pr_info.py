#!/usr/bin/env python3
import requests

class PRInfo:
    def __init__(self, github_event):
        self.number = github_event['number']
        self.sha = github_event['after']
        self.labels = set([l['name'] for l in github_event['pull_request']['labels']])
        self.user_login = github_event['pull_request']['user']['login']
        user_orgs_response = requests.get(github_event['pull_request']['user']['organizations_url'])
        if user_orgs_response.ok:
            response_json = user_orgs_response.json()
            self.user_orgs = set(org['id'] for org in response_json)
        else:
            self.user_orgs = set([])
