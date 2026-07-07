import re

from ci.praktika.info import Info
from ci.praktika.runtime import RunConfig


def get_ci_tags(pr_body, tag_prefix):
    pattern = rf"(- \[x\] +<!---{tag_prefix}_)([|\w]+)"
    matches = []
    for match in re.findall(pattern, pr_body):
        matches.extend(match[-1].split("|"))
    return matches


if __name__ == "__main__":
    info = Info()

    ci_exclude_tags = get_ci_tags(info.pr_body, "ci_exclude")
    ci_regression_jobs = get_ci_tags(info.pr_body, "ci_regression")

    # Store the tags in workflow_config.custom_data, which is emitted as plain
    # JSON in the config job's `data` output. This is the single source of truth
    # for the tags: the GitHub Actions `if:` expressions that gate the regression
    # jobs read `custom_data.ci_(exclude_tags|regression_jobs)`, and the config
    # job's own hooks (filter_job.py, native_jobs.py) read custom_data too.
    # JOB_KV_DATA is not usable here because it is base64-encoded in that output
    # (to keep user-authored strings from matching a secret pattern and
    # suppressing the whole output), so `fromJson(...).JOB_KV_DATA` cannot be
    # dereferenced from a workflow.
    workflow_config = RunConfig.from_fs(info.workflow_name)
    workflow_config.custom_data["ci_exclude_tags"] = ci_exclude_tags
    workflow_config.custom_data["ci_regression_jobs"] = ci_regression_jobs
    workflow_config.dump()
