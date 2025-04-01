import sys

from ci.jobs.scripts.workflow_hooks.pr_description import Labels
from ci.praktika.info import Info


def check():
    info = Info()
    if Labels.CI_PERFORMANCE in info.pr_labels:
        print(f"WARNING: {Labels.CI_PERFORMANCE} label is set, merge not allowed")
        return False
    if Labels.NO_FAST_TESTS in info.pr_labels:
        print(f"WARNING: {Labels.NO_FAST_TESTS} label is set, merge not allowed")
        return False
    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
