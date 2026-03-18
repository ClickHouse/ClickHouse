import sys

from praktika.info import Info

from ci.jobs.scripts.workflow_hooks.pr_description import Labels


def check():
    info = Info()
    if Labels.CI_PERFORMANCE in info.pr_labels:
        print(f"WARNING: {Labels.CI_PERFORMANCE} label is set, merge not allowed")
        return False
    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
