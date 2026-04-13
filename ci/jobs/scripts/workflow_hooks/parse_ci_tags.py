import re

from ci.praktika.info import Info


def get_ci_tags(pr_body, tag_prefix):
    pattern = rf"(- \[x\] +<!---{tag_prefix}_)([|\w]+)"
    matches = []
    for match in re.findall(pattern, pr_body):
        matches.extend(match[-1].split("|"))
    return matches


if __name__ == "__main__":
    info = Info()

    info.store_kv_data("ci_exclude_tags", get_ci_tags(info.pr_body, "ci_exclude"))
    info.store_kv_data("ci_regression_jobs", get_ci_tags(info.pr_body, "ci_regression"))
