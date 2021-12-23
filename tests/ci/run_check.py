#!/usr/bin/env python3
import sys
import logging
from github import Github

from env_helper import GITHUB_RUN_ID, GITHUB_REPOSITORY, GITHUB_SERVER_URL
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from commit_status_helper import get_commit

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
TRUSTED_CONTRIBUTORS = {e.lower() for e in [
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
    "codyrobert",   # Flickerbox engineer
    "cwurm",        # Employee
    "damozhaeva",   # DOCSUP
    "den-crane",
    "flickerbox-tom", # Flickerbox
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
    "zlobober",     # Developer of YT
    "ilejn",        # Arenadata, responsible for Kerberized Kafka
]}


def pr_is_by_trusted_user(pr_user_login, pr_user_orgs):
    if pr_user_login.lower() in TRUSTED_CONTRIBUTORS:
        logging.info("User '%s' is trusted", pr_user_login)
        return True

    logging.info("User '%s' is not trusted", pr_user_login)

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            logging.info("Org '%s' is trusted; will mark user %s as trusted", org_id, pr_user_login)
            return True
        logging.info("Org '%s' is not trusted", org_id)

    return False

# Returns whether we should look into individual checks for this PR. If not, it
# can be skipped entirely.
def should_run_checks_for_pr(pr_info):
    # Consider the labels and whether the user is trusted.
    print("Got labels", pr_info.labels)
    force_labels = set(['force tests']).intersection(pr_info.labels)
    if force_labels:
        return True, "Labeled '{}'".format(', '.join(force_labels))

    if 'do not test' in pr_info.labels:
        return False, "Labeled 'do not test'"

    if 'can be tested' not in pr_info.labels and not pr_is_by_trusted_user(pr_info.user_login, pr_info.user_orgs):
        return False, "Needs 'can be tested' label"

    if 'release' in pr_info.labels or 'pr-backport' in pr_info.labels or 'pr-cherrypick' in pr_info.labels:
        return False, "Don't try new checks for release/backports/cherry-picks"

    return True, "No special conditions apply"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True, labels_from_api=True)
    can_run, description = should_run_checks_for_pr(pr_info)
    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)
    url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"
    if not can_run:
        print("::notice ::Cannot run")
        commit.create_status(context=NAME, description=description, state="failure", target_url=url)
        sys.exit(1)
    else:
        if 'pr-documentation' in pr_info.labels or 'pr-doc-fix' in pr_info.labels:
            commit.create_status(context=NAME, description="Skipping checks for documentation", state="success", target_url=url)
            print("::notice ::Can run, but it's documentation PR, skipping")
        else:
            print("::notice ::Can run")
            commit.create_status(context=NAME, description=description, state="pending", target_url=url)
