import sys

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info

# With these labels the PR workflow runs a subset of its jobs: the pull request merges after the
# full CI, without the label.
FORBIDDEN_LABELS = [
    Labels.CI_PERFORMANCE,
    Labels.CI_INTEGRATION_FLAKY,
    Labels.CI_FUNCTIONAL_FLAKY,
    Labels.CI_INTEGRATION,
    Labels.CI_FUNCTIONAL,
    Labels.CI_BUILD,
]

STATELESS_TESTS_DIR = "tests/queries/0_stateless/"


def llvm_coverage_label_allows_merge(changed_files):
    """`ci-llvm-coverage` runs a subset of the jobs too (see `LLVM_COVERAGE_ONLY_JOBS` in
    `filter_job.py`), but a pull request that changes only stateless tests may merge with it:
    its binary is identical to master's, so the skipped builds, stress tests, fuzzers and
    performance tests would find nothing new, while the coverage shards run the whole suite in
    every configuration and the flaky checks run the changed tests under every sanitizer. Any
    other change needs the full CI. Unknown changed files block the merge (fail-close)."""
    if not changed_files:
        return False
    return all(
        f.removeprefix(".").removeprefix("/").startswith(STATELESS_TESTS_DIR)
        for f in changed_files
    )


def check():
    info = Info()
    for label in FORBIDDEN_LABELS:
        if label in info.pr_labels:
            print(f"WARNING: {label} label is set, merge not allowed")
            return False

    if Labels.CI_LLVM_COVERAGE in info.pr_labels and not llvm_coverage_label_allows_merge(
        info.get_changed_files()
    ):
        print(
            f"WARNING: {Labels.CI_LLVM_COVERAGE} label is set and the pull request changes "
            f"files outside {STATELESS_TESTS_DIR}, merge not allowed"
        )
        return False

    return True


if __name__ == "__main__":
    if not check():
        sys.exit(1)
