#!/usr/bin/env python3
import sys
import logging
import re
from typing import Tuple

from github import Github

from commit_status_helper import (
    CI_STATUS_NAME,
    format_description,
    get_commit,
    post_commit_status,
    post_labels,
    remove_labels,
    set_mergeable_check,
)
from docs_check import NAME as DOCS_NAME
from env_helper import GITHUB_RUN_URL, GITHUB_REPOSITORY, GITHUB_SERVER_URL
from get_robot_token import get_best_robot_token
from pr_info import FORCE_TESTS_LABEL, PRInfo
from workflow_approve_rerun_lambda.app import TRUSTED_CONTRIBUTORS

TRUSTED_ORG_IDS = {
    54801242,  # clickhouse
}

OK_SKIP_LABELS = {"release", "pr-backport", "pr-cherrypick"}
CAN_BE_TESTED_LABEL = "can be tested"
DO_NOT_TEST_LABEL = "do not test"
FEATURE_LABEL = "pr-feature"
SUBMODULE_CHANGED_LABEL = "submodule changed"

# They are used in .github/PULL_REQUEST_TEMPLATE.md, keep comments there
# updated accordingly
# The following lists are append only, try to avoid editing them
# They atill could be cleaned out after the decent time though.
LABELS = {
    "pr-backward-incompatible": ["Backward Incompatible Change"],
    "pr-bugfix": [
        "Bug Fix",
        "Bug Fix (user-visible misbehavior in an official stable release)",
        "Bug Fix (user-visible misbehaviour in official stable or prestable release)",
        "Bug Fix (user-visible misbehavior in official stable or prestable release)",
    ],
    "pr-build": [
        "Build/Testing/Packaging Improvement",
        "Build Improvement",
        "Build/Testing Improvement",
        "Build",
        "Packaging Improvement",
    ],
    "pr-documentation": [
        "Documentation (changelog entry is not required)",
        "Documentation",
    ],
    "pr-feature": ["New Feature"],
    "pr-improvement": ["Improvement"],
    "pr-not-for-changelog": [
        "Not for changelog (changelog entry is not required)",
        "Not for changelog",
    ],
    "pr-performance": ["Performance Improvement"],
}

CATEGORY_TO_LABEL = {c: lb for lb, categories in LABELS.items() for c in categories}


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
def should_run_ci_for_pr(pr_info: PRInfo) -> Tuple[bool, str, str]:
    # Consider the labels and whether the user is trusted.
    print("Got labels", pr_info.labels)
    if FORCE_TESTS_LABEL in pr_info.labels:
        print(f"Label '{FORCE_TESTS_LABEL}' set, forcing remaining checks")
        return True, f"Labeled '{FORCE_TESTS_LABEL}'", "pending"

    if DO_NOT_TEST_LABEL in pr_info.labels:
        print(f"Label '{DO_NOT_TEST_LABEL}' set, skipping remaining checks")
        return False, f"Labeled '{DO_NOT_TEST_LABEL}'", "success"

    if OK_SKIP_LABELS.intersection(pr_info.labels):
        return (
            True,
            "Don't try new checks for release/backports/cherry-picks",
            "success",
        )

    if CAN_BE_TESTED_LABEL not in pr_info.labels and not pr_is_by_trusted_user(
        pr_info.user_login, pr_info.user_orgs
    ):
        print(
            f"PRs by untrusted users need the '{CAN_BE_TESTED_LABEL}' label - please contact a member of the core team"
        )
        return False, "Needs 'can be tested' label", "failure"

    return True, "No special conditions apply", "pending"


def check_pr_description(pr_info: PRInfo) -> Tuple[str, str]:
    lines = list(
        map(lambda x: x.strip(), pr_info.body.split("\n") if pr_info.body else [])
    )
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    # Check if body contains "Reverts ClickHouse/ClickHouse#36337"
    if [
        True
        for line in lines
        if re.match(rf"\AReverts {GITHUB_REPOSITORY}#[\d]+\Z", line)
    ]:
        return "", LABELS["pr-not-for-changelog"][0]

    category = ""
    entry = ""
    description_error = ""

    i = 0
    while i < len(lines):
        if re.match(r"(?i)^[#>*_ ]*change\s*log\s*category", lines[i]):
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
                return result_status, category

        elif re.match(
            r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
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
            # Don't accept changelog entries like 'Close #12345'.
            entry = re.sub(r"^[\w\-\s]{0,10}#?\d{5,6}\.?$", "", entry)
        else:
            i += 1

    if not category:
        description_error = "Changelog category is empty"
    # Filter out the PR categories that are not for changelog.
    elif re.match(
        r"(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)",
        category,
    ):
        pass  # to not check the rest of the conditions
    elif category not in CATEGORY_TO_LABEL:
        description_error, category = f"Category '{category}' is not valid", ""
    elif not entry:
        description_error = f"Changelog entry required for category '{category}'"

    return description_error, category


def main():
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True, pr_event_from_api=True, need_changed_files=True)
    # The case for special branches like backports and releases without created
    # PRs, like merged backport branches that are reset immediately after merge
    if pr_info.number == 0:
        print("::notice ::Cannot run, no PR exists for the commit")
        sys.exit(1)

    can_run, description, labels_state = should_run_ci_for_pr(pr_info)
    if can_run and OK_SKIP_LABELS.intersection(pr_info.labels):
        print("::notice :: Early finish the check, running in a special PR")
        sys.exit(0)

    description = format_description(description)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    description_error, category = check_pr_description(pr_info)
    pr_labels_to_add = []
    pr_labels_to_remove = []
    if (
        category in CATEGORY_TO_LABEL
        and CATEGORY_TO_LABEL[category] not in pr_info.labels
    ):
        pr_labels_to_add.append(CATEGORY_TO_LABEL[category])

    for label in pr_info.labels:
        if (
            label in CATEGORY_TO_LABEL.values()
            and category in CATEGORY_TO_LABEL
            and label != CATEGORY_TO_LABEL[category]
        ):
            pr_labels_to_remove.append(label)

    if pr_info.has_changes_in_submodules():
        pr_labels_to_add.append(SUBMODULE_CHANGED_LABEL)
    elif SUBMODULE_CHANGED_LABEL in pr_info.labels:
        pr_labels_to_remove.append(SUBMODULE_CHANGED_LABEL)

    print(f"Change labels: add {pr_labels_to_add}, remove {pr_labels_to_remove}")
    if pr_labels_to_add:
        post_labels(gh, pr_info, pr_labels_to_add)

    if pr_labels_to_remove:
        remove_labels(gh, pr_info, pr_labels_to_remove)

    if FEATURE_LABEL in pr_info.labels:
        print(f"The '{FEATURE_LABEL}' in the labels, expect the 'Docs Check' status")
        post_commit_status(  # do not pass pr_info here intentionally
            commit,
            "pending",
            "",
            f"expect adding docs for {FEATURE_LABEL}",
            DOCS_NAME,
        )
    else:
        set_mergeable_check(commit, "skipped")

    if description_error:
        print(
            "::error ::Cannot run, PR description does not match the template: "
            f"{description_error}"
        )
        logging.info(
            "PR body doesn't match the template: (start)\n%s\n(end)\nReason: %s",
            pr_info.body,
            description_error,
        )
        url = (
            f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/"
            "blob/master/.github/PULL_REQUEST_TEMPLATE.md?plain=1"
        )
        post_commit_status(
            commit,
            "failure",
            url,
            format_description(description_error),
            CI_STATUS_NAME,
            pr_info,
        )
        sys.exit(1)

    url = GITHUB_RUN_URL
    if not can_run:
        print("::notice ::Cannot run")
        post_commit_status(
            commit, labels_state, url, description, CI_STATUS_NAME, pr_info
        )
        sys.exit(1)
    else:
        print("::notice ::Can run")
        post_commit_status(commit, "pending", url, description, CI_STATUS_NAME, pr_info)


if __name__ == "__main__":
    main()
