#!/usr/bin/env python3
import os
import json
import requests
from pr_info import PRInfo
import sys
import logging
from github import Github

NAME = 'Run Check (actions)'

TRUSTED_ORG_IDS = {
    7409213,   # yandex
    28471076,  # altinity
    54801242,  # clickhouse
}

OK_TEST_LABEL = set(["can be tested", "release", "pr-documentation", "pr-doc-fix"])
DO_NOT_TEST_LABEL = "do not test"

# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    "achimbab",
    "adevyatova ",  # DOCSUP
    "Algunenano",   # Raúl Marín, Tinybird
    "AnaUvarova",   # DOCSUP
    "anauvarova",   # technical writer, Yandex
    "annvsh",       # technical writer, Yandex
    "atereh",       # DOCSUP
    "azat",
    "bharatnc",     # Newbie, but already with many contributions.
    "bobrik",       # Seasoned contributor, CloundFlare
    "BohuTANG",
    "damozhaeva",   # DOCSUP
    "den-crane",
    "gyuton",       # DOCSUP
    "gyuton",       # technical writer, Yandex
    "hagen1778",    # Roman Khavronenko, seasoned contributor
    "hczhcz",
    "hexiaoting",   # Seasoned contributor
    "ildus",        # adjust, ex-pgpro
    "javisantana",  # a Spanish ClickHouse enthusiast, ex-Carto
    "ka1bi4",       # DOCSUP
    "kirillikoff",  # DOCSUP
    "kitaisreal",   # Seasoned contributor
    "kreuzerkrieg",
    "lehasm",       # DOCSUP
    "michon470",    # DOCSUP
    "MyroTk",       # Tester in Altinity
    "myrrc",        # Michael Kot, Altinity
    "nikvas0",
    "nvartolomei",
    "olgarev",      # DOCSUP
    "otrazhenia",   # Yandex docs contractor
    "pdv-ru",       # DOCSUP
    "podshumok",    # cmake expert from QRator Labs
    "s-mx",         # Maxim Sabyanin, former employee, present contributor
    "sevirov",      # technical writer, Yandex
    "spongedu",     # Seasoned contributor
    "ucasFL",       # Amos Bird's friend
    "vdimir",       # Employee
    "vzakaznikov",
    "YiuRULE",
    "zlobober"      # Developer of YT
}


def pr_is_by_trusted_user(pr_user_login, pr_user_orgs):
    if pr_user_login in TRUSTED_CONTRIBUTORS:
        logging.info("User '{}' is trusted".format(user))
        return True

    logging.info("User '{}' is not trusted".format(user))

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            logging.info("Org '{}' is trusted; will mark user {} as trusted".format(org_id, user))
            return True
        logging.info("Org '{}' is not trusted".format(org_id))

    return False

# Returns whether we should look into individual checks for this PR. If not, it
# can be skipped entirely.
def should_run_checks_for_pr(pr_info):
    # Consider the labels and whether the user is trusted.
    force_labels = set(['force tests', 'release']).intersection(pr_info.labels)
    if force_labels:
        return True, "Labeled '{}'".format(', '.join(force_labels))

    if 'do not test' in pr_info.labels:
        return False, "Labeled 'do not test'"

    if 'can be tested' not in pr_info.labels and not pr_is_by_trusted_user(pr_info.user_login, pr_info.user_orgs):
        return False, "Needs 'can be tested' label"

    # Stop processing any checks for a PR when Fast Test fails.
    fast_test_status = pr_info.statuses.get("Fast Test")
    if fast_test_status and fast_test_status.state == 'failure':
        return False, "Fast Test has failed"

    return True, "No special conditions apply"

def get_commit(gh, commit_sha):
    repo = gh.get_repo(os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse"))
    commit = repo.get_commit(commit_sha)
    return commit

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    with open(os.getenv('GITHUB_EVENT_PATH'), 'r') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event)
    can_run, description = should_run_checks_for_pr(pr_info)
    if not can_run:
        task_url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
        print("Commit sha", pr_info.sha)
        print("PR number", pr_info.number)
        gh = Github(os.getenv("GITHUB_TOKEN"))
        commit = get_commit(gh, pr_info.sha)
        url = f"https://github.com/ClickHouse/ClickHouse/actions/runs/{os.getenv('GITHUB_RUN_ID')}"
        commit.create_status(context=NAME, description=description, state="failure", target_url=url)
        sys.exit(1)
