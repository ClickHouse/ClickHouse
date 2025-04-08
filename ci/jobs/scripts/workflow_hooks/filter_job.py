from ci.defs.defs import JobNames
from ci.jobs.scripts.workflow_hooks.pr_description import Labels
from ci.praktika.info import Info


def only_docs(changed_files):
    for file in changed_files:
        file = file.removeprefix(".").removeprefix("/")
        if (
            file.startswith("docs/")
            or file.startswith("docker/docs")
            or file.endswith(".md")
            or "aspell-dict.txt" in file
        ):
            continue
        else:
            return False
    return True


ONLY_DOCS_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.DOCKER_BUILDS_ARM,
    JobNames.DOCKER_BUILDS_AMD,
    JobNames.Docs,
]

PRELIMINARY_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    "Build (amd_tidy)",
    "Build (arm_tidy)",
]

_info_cache = None


def should_skip_job(job_name):
    global _info_cache
    if _info_cache is None:
        _info_cache = Info()

    changed_files = _info_cache.get_custom_data("changed_files")
    if not changed_files:
        print("WARNING: no changed files found for PR - do not filter jobs")
        return False, ""

    if only_docs(changed_files) and job_name not in ONLY_DOCS_JOBS:
        return True, "Docs only update"

    if Labels.DO_NOT_TEST in _info_cache.pr_labels and job_name not in ONLY_DOCS_JOBS:
        return True, f"Skipped, labeled with '{Labels.DO_NOT_TEST}'"

    if Labels.NO_FAST_TESTS in _info_cache.pr_labels and job_name in PRELIMINARY_JOBS:
        return True, f"Skipped, labeled with '{Labels.NO_FAST_TESTS}'"

    if Labels.CI_PERFORMANCE in _info_cache.pr_labels and (
        "performance" not in job_name.lower()
        and job_name
        not in (
            "Build (amd_release)",
            "Build (arm_release)",
            JobNames.DOCKER_BUILDS_ARM,
            JobNames.DOCKER_BUILDS_AMD,
        )
    ):
        return (
            True,
            "Skipped, labeled with 'ci-performance' - run performance jobs only",
        )

    if "- Bug Fix" not in _info_cache.pr_body and JobNames.BUGFIX_VALIDATE in job_name:
        return True, "Skipped, not a bug-fix PR"

    # skip ARM perf tests for non-performance update
    if (
        # Labels.PR_PERFORMANCE not in _info_cache.pr_labels
        "- Performance Improvement" not in _info_cache.pr_body
        and JobNames.PERFORMANCE in job_name
        and "arm" in job_name
    ):
        return True, "Skipped, not labeled with 'pr-performance'"

    return False, ""
