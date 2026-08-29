import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info


def check():
    info = Info()
    forbidden_labels = [
        Labels.CI_PERFORMANCE,
        Labels.NO_FAST_TESTS,
        Labels.CI_INTEGRATION_FLAKY,
        Labels.CI_FUNCTIONAL_FLAKY,
        Labels.CI_INTEGRATION,
        Labels.CI_FUNCTIONAL,
        Labels.CI_BUILD,
    ]

    for label in forbidden_labels:
        if label in info.pr_labels:
            print(f"WARNING: {label} label is set, merge not allowed")
            return False

    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
