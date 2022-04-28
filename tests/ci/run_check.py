#!/usr/bin/env python3
import sys
import logging
import re
from typing import Tuple

from github import Github
from env_helper import GITHUB_RUN_URL, GITHUB_REPOSITORY, GITHUB_SERVER_URL
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from commit_status_helper import get_commit

NAME = "Run Check (actions)"

TRUSTED_ORG_IDS = {
    7409213,  # yandex
    28471076,  # altinity
    54801242,  # clickhouse
}

OK_SKIP_LABELS = {"release", "pr-backport", "pr-cherrypick"}
CAN_BE_TESTED_LABEL = "can be tested"
DO_NOT_TEST_LABEL = "do not test"
FORCE_TESTS_LABEL = "force tests"

# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "achimbab",
        "adevyatova ",  # DOCSUP
        "Algunenano",  # Raúl Marín, Tinybird
        "amosbird",
        "AnaUvarova",  # DOCSUP
        "anauvarova",  # technical writer, Yandex
        "annvsh",  # technical writer, Yandex
        "atereh",  # DOCSUP
        "azat",
        "bharatnc",  # Newbie, but already with many contributions.
        "bobrik",  # Seasoned contributor, CloudFlare
        "BohuTANG",
        "codyrobert",  # Flickerbox engineer
        "cwurm",  # Employee
        "damozhaeva",  # DOCSUP
        "den-crane",
        "flickerbox-tom",  # Flickerbox
        "gyuton",  # technical writer, Yandex
        "hagen1778",  # Roman Khavronenko, seasoned contributor
        "hczhcz",
        "hexiaoting",  # Seasoned contributor
        "ildus",  # adjust, ex-pgpro
        "javisantana",  # a Spanish ClickHouse enthusiast, ex-Carto
        "ka1bi4",  # DOCSUP
        "kirillikoff",  # DOCSUP
        "kitaisreal",  # Seasoned contributor
        "kreuzerkrieg",
        "lehasm",  # DOCSUP
        "michon470",  # DOCSUP
        "MyroTk",  # Tester in Altinity
        "myrrc",  # Michael Kot, Altinity
        "nikvas0",
        "nvartolomei",
        "olgarev",  # DOCSUP
        "otrazhenia",  # Yandex docs contractor
        "pdv-ru",  # DOCSUP
        "podshumok",  # cmake expert from QRator Labs
        "s-mx",  # Maxim Sabyanin, former employee, present contributor
        "sevirov",  # technical writer, Yandex
        "spongedu",  # Seasoned contributor
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "vdimir",  # Employee
        "vzakaznikov",
        "YiuRULE",
        "zlobober",  # Developer of YT
        "ilejn",  # Arenadata, responsible for Kerberized Kafka
        "thomoco",  # ClickHouse
        "BoloniniD",  # Seasoned contributor, HSE
        "arthurpassos" # Altinity
    ]
}


def pr_is_by_trusted_user(pr_user_login, pr_user_orgs):
    if pr_user_login.lower() in TRUSTED_CONTRIBUTORS:
        logging.info("User '%s' is trusted", pr_user_login)
        return True

    logging.info("User '%s' is not trusted", pr_user_login)

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            logging.info(
                "Org '%s' is trusted; will mark user %s as trusted",
                org_id,
                pr_user_login,
            )
            return True
        logging.info("Org '%s' is not trusted", org_id)

    return False


# Returns whether we should look into individual checks for this PR. If not, it
# can be skipped entirely.
# Returns can_run, description, labels_state
def should_run_checks_for_pr(pr_info: PRInfo) -> Tuple[bool, str, str]:
    # Consider the labels and whether the user is trusted.
    print("Got labels", pr_info.labels)
    if FORCE_TESTS_LABEL in pr_info.labels:
        return True, f"Labeled '{FORCE_TESTS_LABEL}'", "pending"

    if DO_NOT_TEST_LABEL in pr_info.labels:
        return False, f"Labeled '{DO_NOT_TEST_LABEL}'", "success"

    if CAN_BE_TESTED_LABEL not in pr_info.labels and not pr_is_by_trusted_user(
        pr_info.user_login, pr_info.user_orgs
    ):
        return False, "Needs 'can be tested' label", "failure"

    if OK_SKIP_LABELS.intersection(pr_info.labels):
        return (
            False,
            "Don't try new checks for release/backports/cherry-picks",
            "success",
        )

    return True, "No special conditions apply", "pending"


def check_pr_description(pr_info):
    description = pr_info.body

    lines = list(
        map(lambda x: x.strip(), description.split("\n") if description else [])
    )
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    category = ""
    entry = ""

    i = 0
    while i < len(lines):
        if re.match(r"(?i)^[>*_ ]*change\s*log\s*category", lines[i]):
            i += 1
            if i >= len(lines):
                break
            # Can have one empty line between header and the category
            # itself. Filter it out.
            if not lines[i]:
                i += 1
                if i >= len(lines):
                    break
            category = re.sub(r"^[-*\s]*", "", lines[i])
            i += 1

            # Should not have more than one category. Require empty line
            # after the first found category.
            if i >= len(lines):
                break
            if lines[i]:
                second_category = re.sub(r"^[-*\s]*", "", lines[i])
                result_status = (
                    "More than one changelog category specified: '"
                    + category
                    + "', '"
                    + second_category
                    + "'"
                )
                return result_status[:140]

        elif re.match(
            r"(?i)^[>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
        ):
            i += 1
            # Can have one empty line between header and the entry itself.
            # Filter it out.
            if i < len(lines) and not lines[i]:
                i += 1
            # All following lines until empty one are the changelog entry.
            entry_lines = []
            while i < len(lines) and lines[i]:
                entry_lines.append(lines[i])
                i += 1
            entry = " ".join(entry_lines)
            # Don't accept changelog entries like '...'.
            entry = re.sub(r"[#>*_.\- ]", "", entry)
        else:
            i += 1

    if not category:
        return "Changelog category is empty"

    # Filter out the PR categories that are not for changelog.
    if re.match(
        r"(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)",
        category,
    ):
        return ""

    if not entry:
        return f"Changelog entry required for category '{category}'"

    return ""


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True, pr_event_from_api=True)
    can_run, description, labels_state = should_run_checks_for_pr(pr_info)
    gh = Github(get_best_robot_token())
    commit = get_commit(gh, pr_info.sha)

    description_report = check_pr_description(pr_info)[:139]
    if description_report:
        print("::notice ::Cannot run, description does not match the template")
        logging.info(
            "PR body doesn't match the template: (start)\n%s\n(end)", pr_info.body
        )
        url = (
            f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/"
            "blob/master/.github/PULL_REQUEST_TEMPLATE.md?plain=1"
        )
        commit.create_status(
            context=NAME,
            description=description_report,
            state="failure",
            target_url=url,
        )
        sys.exit(1)

    url = GITHUB_RUN_URL
    if not can_run:
        print("::notice ::Cannot run")
        commit.create_status(
            context=NAME, description=description, state=labels_state, target_url=url
        )
        sys.exit(1)
    else:
        if "pr-documentation" in pr_info.labels or "pr-doc-fix" in pr_info.labels:
            commit.create_status(
                context=NAME,
                description="Skipping checks for documentation",
                state="success",
                target_url=url,
            )
            print("::notice ::Can run, but it's documentation PR, skipping")
        else:
            print("::notice ::Can run")
            commit.create_status(
                context=NAME, description=description, state="pending", target_url=url
            )
