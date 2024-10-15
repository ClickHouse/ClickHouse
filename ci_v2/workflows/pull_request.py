from typing import List

from ci_v2.settings.definitions import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    JobNames,
    RunnerLabels,
)
from praktika import Job, Workflow

style_check_job = Job.Config(
    name=JobNames.STYLE_CHECK,
    runs_on=[RunnerLabels.CI_SERVICES],
    command="python3 ./ci_v2/jobs/check_style.py",
    run_in_docker="clickhouse/style-test",
)

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        style_check_job,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_merge_ready_status=True,
)

WORKFLOWS = [
    workflow,
]  # type: List[Workflow.Config]


if __name__ == "__main__":
    # example: local job test inside praktika environment
    from praktika.runner import Runner

    Runner.generate_dummy_environment(workflow, style_check_job)

    Runner().run(workflow, style_check_job)
