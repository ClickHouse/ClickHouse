#!/usr/bin/env python3
import logging
import sys
from typing import Tuple

from github import Github

from cherry_pick import Labels
from commit_status_helper import (
    CI_STATUS_NAME,
    create_ci_report,
    format_description,
    get_commit,
    post_commit_status,
    post_labels,
    remove_labels,
)
from env_helper import GITHUB_REPOSITORY, GITHUB_SERVER_URL
from get_robot_token import get_best_robot_token
from lambda_shared_package.lambda_shared.pr import (
    CATEGORY_TO_LABEL,
    TRUSTED_CONTRIBUTORS,
    check_pr_description,
)
from pr_info import PRInfo
from report import FAILURE, PENDING, SUCCESS

TRUSTED_ORG_IDS = {
    54801242,  # clickhouse
}

OK_SKIP_LABELS = {"release", "pr-backport", "pr-cherrypick"}
CAN_BE_TESTED_LABEL = "can be tested"
FEATURE_LABEL = "pr-feature"
SUBMODULE_CHANGED_LABEL = "submodule changed"
PR_CHECK = "PR Check"
# pr-bugfix autoport can lead to issues in releases, let's do ci fixes only
AUTO_BACKPORT_LABELS = ["pr-ci"]


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
    print("Got labels", pr_info.labels)

    if OK_SKIP_LABELS.intersection(pr_info.labels):
        return True, "Don't try new checks for release/backports/cherry-picks"

    if CAN_BE_TESTED_LABEL not in pr_info.labels and not pr_is_by_trusted_user(
        pr_info.user_login, pr_info.user_orgs
    ):
        print(
            f"PRs by untrusted users need the '{CAN_BE_TESTED_LABEL}' label - please contact a member of the core team"
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
        pr_labels_to_add.append(SUBMODULE_CHANGED_LABEL)
    elif SUBMODULE_CHANGED_LABEL in pr_info.labels:
        pr_labels_to_remove.append(SUBMODULE_CHANGED_LABEL)

    if any(label in AUTO_BACKPORT_LABELS for label in pr_labels_to_add):
        backport_labels = [Labels.MUST_BACKPORT, Labels.MUST_BACKPORT_CLOUD]
        pr_labels_to_add += [
            label for label in backport_labels if label not in pr_info.labels
        ]
        print(
            f"::notice :: Add backport labels [{backport_labels}] for a given PR category"
        )

    print(f"Change labels: add {pr_labels_to_add}, remove {pr_labels_to_remove}")
    if pr_labels_to_add:
        post_labels(gh, pr_info, pr_labels_to_add)

    if pr_labels_to_remove:
        remove_labels(gh, pr_info, pr_labels_to_remove)

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
            FAILURE,
            url,
            format_description(description_error),
            PR_CHECK,
            pr_info,
        )
        sys.exit(1)

    if FEATURE_LABEL in pr_info.labels and not pr_info.has_changes_in_documentation():
        print(
            f"The '{FEATURE_LABEL}' in the labels, "
            "but there's no changed documentation"
        )
        post_commit_status(
            commit,
            FAILURE,
            "",
            f"expect adding docs for {FEATURE_LABEL}",
            PR_CHECK,
            pr_info,
        )
        # allow the workflow to continue

    if not can_run:
        post_commit_status(
            commit,
            FAILURE,
            "",
            description,
            PR_CHECK,
            pr_info,
        )
        print("::notice ::Cannot run")
        sys.exit(1)

    post_commit_status(
        commit,
        SUCCESS,
        "",
        "ok",
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
            CI_STATUS_NAME,
            pr_info,
        )


if __name__ == "__main__":
    main()
