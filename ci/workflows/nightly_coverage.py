from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs
from ci.praktika.info import Info


def filter_coverage_jobs(job_name):
    info = Info()
    if not info.is_dispatch_event:
        return False, ""
    suite = info.get_workflow_input_value("suite")
    if suite == "all":
        return False, ""
    if suite != "performance":
        raise ValueError(f"Unknown coverage suite: {suite!r}")
    selected = {
        JobConfigs.coverage_build_jobs[1].name,
        *(job.name for job in JobConfigs.performance_coverage_jobs),
    }
    return job_name not in selected, "Only performance coverage was requested"


workflow = Workflow.Config(
    name="NightlyCoverage",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        JobConfigs.coverage_build_jobs[
            1
        ],  # Per-test LLVM entry counters with randomized settings
        *JobConfigs.functional_tests_jobs_coverage,
        *JobConfigs.integration_test_per_test_coverage_jobs,
        *JobConfigs.performance_coverage_jobs,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    artifacts=ArtifactConfigs.clickhouse_binaries,
    enable_cache=True,
    enable_report=True,
    enable_slack_feed=True,
    enable_cidb=True,
    cron_schedules=["13 2 * * *"],
    inputs=[
        Workflow.Config.InputConfig(
            name="suite",
            description="Coverage suite to collect",
            is_required=True,
            default_value="all",
            options=["all", "performance"],
        ),
    ],
    workflow_filter_hooks=[filter_coverage_jobs],
)

WORKFLOWS = [
    workflow,
]
