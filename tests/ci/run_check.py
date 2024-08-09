#!/usr/bin/env python3
import logging
import re
import sys
from typing import Tuple

from github import Github

from commit_status_helper import (
    create_ci_report,
    format_description,
    get_commit,
    post_commit_status,
    post_labels,
    remove_labels,
)
from env_helper import GITHUB_REPOSITORY, GITHUB_SERVER_URL
from get_robot_token import get_best_robot_token
from ci_config import CI
from pr_info import PRInfo
from report import FAILURE, PENDING, SUCCESS, StatusType


TRUSTED_ORG_IDS = {
    54801242,  # clickhouse
}

TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "amosbird",
        "azat",  # SEMRush
        "bharatnc",  # Many contributions.
        "cwurm",  # ClickHouse, Inc
        "den-crane",  # Documentation contributor
        "ildus",  # adjust, ex-pgpro
        "nvartolomei",  # Seasoned contributor, CloudFlare
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "thomoco",  # ClickHouse, Inc
        "tonickkozlov",  # Cloudflare
        "tylerhannan",  # ClickHouse, Inc
        "tsolodov",  # ClickHouse, Inc
        "justindeguzman",  # ClickHouse, Inc
        "XuJia0210",  # ClickHouse, Inc
    ]
}

OK_SKIP_LABELS = {CI.Labels.RELEASE, CI.Labels.PR_BACKPORT, CI.Labels.PR_CHERRYPICK}
PR_CHECK = "PR Check"


LABEL_CATEGORIES = {
    "pr-backward-incompatible": ["Backward Incompatible Change"],
    "pr-bugfix": [
        "Bug Fix",
        "Bug Fix (user-visible misbehavior in an official stable release)",
        "Bug Fix (user-visible misbehaviour in official stable or prestable release)",
        "Bug Fix (user-visible misbehavior in official stable or prestable release)",
    ],
    "pr-critical-bugfix": ["Critical Bug Fix (crash, LOGICAL_ERROR, data loss, RBAC)"],
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
    "pr-ci": ["CI Fix or Improvement (changelog entry is not required)"],
}

CATEGORY_TO_LABEL = {
    c: lb for lb, categories in LABEL_CATEGORIES.items() for c in categories
}


def check_pr_description(pr_body: str, repo_name: str) -> Tuple[str, str]:
    """The function checks the body to being properly formatted according to
    .github/PULL_REQUEST_TEMPLATE.md, if the first returned string is not empty,
    then there is an error."""
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    # Check if body contains "Reverts ClickHouse/ClickHouse#36337"
    if [True for line in lines if re.match(rf"\AReverts {repo_name}#[\d]+\Z", line)]:
        return "", LABEL_CATEGORIES["pr-not-for-changelog"][0]

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
                description_error = (
                    "More than one changelog category specified: "
                    f"'{category}', '{second_category}'"
                )
                return description_error, category

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
    elif "(changelog entry is not required)" in category:
        pass  # to not check the rest of the conditions
    elif category not in CATEGORY_TO_LABEL:
        description_error, category = f"Category '{category}' is not valid", ""
    elif not entry:
        description_error = f"Changelog entry required for category '{category}'"

    return description_error, category


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
# Returns can_run, description
def should_run_ci_for_pr(pr_info: PRInfo) -> Tuple[bool, str]:
    # Consider the labels and whether the user is trusted.
    logging.info("Got labels: %s", pr_info.labels)

    if OK_SKIP_LABELS.intersection(pr_info.labels):
        return True, "Don't try new checks for release/backports/cherry-picks"

    if CI.Labels.CAN_BE_TESTED not in pr_info.labels and not pr_is_by_trusted_user(
        pr_info.user_login, pr_info.user_orgs
    ):
        logging.info(
            "PRs by untrusted users need the '%s' label - "
            "please contact a member of the core team",
            CI.Labels.CAN_BE_TESTED,
        )
        return False, "Needs 'can be tested' label"

    return True, "No special conditions apply"


def main():
    logging.basicConfig(level=logging.INFO)

    pr_info = PRInfo(need_orgs=True, pr_event_from_api=True, need_changed_files=True)
    # The case for special branches like backports and releases without created
    # PRs, like merged backport branches that are reset immediately after merge
    if pr_info.number == 0:
        print("::notice ::Cannot run, no PR exists for the commit")
        sys.exit(1)

    can_run, description = should_run_ci_for_pr(pr_info)
    if can_run and OK_SKIP_LABELS.intersection(pr_info.labels):
        print("::notice :: Early finish the check, running in a special PR")
        sys.exit(0)

    description = format_description(description)
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)
    status = SUCCESS  # type: StatusType

    description_error, category = check_pr_description(pr_info.body, GITHUB_REPOSITORY)
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
        pr_labels_to_add.append(CI.Labels.SUBMODULE_CHANGED)
    elif CI.Labels.SUBMODULE_CHANGED in pr_info.labels:
        pr_labels_to_remove.append(CI.Labels.SUBMODULE_CHANGED)

    if any(label in CI.Labels.AUTO_BACKPORT for label in pr_labels_to_add):
        backport_labels = [CI.Labels.MUST_BACKPORT, CI.Labels.MUST_BACKPORT_CLOUD]
        pr_labels_to_add += [
            label for label in backport_labels if label not in pr_info.labels
        ]
        print(
            f"::notice :: Add backport labels [{backport_labels}] for a given PR category"
        )

    logging.info(
        "Change labels: add %s, remove %s", pr_labels_to_add, pr_labels_to_remove
    )
    if pr_labels_to_add:
        post_labels(gh, pr_info, pr_labels_to_add)

    if pr_labels_to_remove:
        remove_labels(gh, pr_info, pr_labels_to_remove)

    # 1. Next three IFs are in a correct order. First - fatal error
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
        status = FAILURE
        post_commit_status(
            commit,
            status,
            url,
            format_description(description_error),
            PR_CHECK,
            pr_info,
        )
        sys.exit(1)

    # 2. Then we check if the documentation is not created to fail the Mergeable check
    if (
        CI.Labels.PR_FEATURE in pr_info.labels
        and not pr_info.has_changes_in_documentation()
    ):
        print(
            f"::error ::The '{CI.Labels.PR_FEATURE}' in the labels, "
            "but there's no changed documentation"
        )
        status = FAILURE
        description = f"expect adding docs for {CI.Labels.PR_FEATURE}"
    # 3. But we allow the workflow to continue

    # 4. And post only a single commit status on a failure
    if not can_run:
        post_commit_status(
            commit,
            status,
            "",
            description,
            PR_CHECK,
            pr_info,
        )
        print("::error ::Cannot run")
        sys.exit(1)

    # The status for continue can be posted only one time, not more.
    post_commit_status(
        commit,
        status,
        "",
        description,
        PR_CHECK,
        pr_info,
    )

    ci_report_url = create_ci_report(pr_info, [])
    print("::notice ::Can run")

    if not pr_info.is_merge_queue:
        # we need clean CI status for MQ to merge (no pending statuses)
        post_commit_status(
            commit,
            PENDING,
            ci_report_url,
            description,
            CI.StatusNames.CI,
            pr_info,
        )


if __name__ == "__main__":
    main()
