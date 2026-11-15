from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs
from ci.jobs.performance_coverage_resume import validate_options
from ci.praktika.info import Info


def filter_coverage_jobs(job_name):
    info = Info()
    if not info.is_dispatch_event:
        return False, ""
    suite = info.get_workflow_input_value("suite")
    shards = info.get_workflow_input_value("performance_shards")
    resume_from = info.get_workflow_input_value("performance_resume_from") or ""
    max_hours = info.get_workflow_input_value("performance_max_hours") or "12"
    binary_url = info.get_workflow_input_value("performance_binary_url") or ""
    binary_sha256 = info.get_workflow_input_value("performance_binary_sha256") or ""
    validate_options(resume_from, max_hours, binary_url, binary_sha256)
    # Older dispatches predate this optional input.
    shards = "all" if shards is None else shards.strip()
    if suite == "all":
        if shards != "all" or resume_from or max_hours != "12" or binary_url:
            raise ValueError("Performance recovery inputs require suite=performance")
        return False, ""
    if suite != "performance":
        raise ValueError(f"Unknown coverage suite: {suite!r}")
    jobs = JobConfigs.performance_coverage_jobs
    if shards == "all":
        selected_shards = set(range(1, len(jobs) + 1))
    else:
        parts = [part.strip() for part in shards.split(",")]
        if not all(part.isascii() and part.isdecimal() for part in parts):
            raise ValueError(
                "performance_shards must be 'all' or comma-separated shard numbers"
            )
        selected_shards = {int(part) for part in parts}
        if not selected_shards <= set(range(1, len(jobs) + 1)):
            raise ValueError(f"performance_shards must be between 1 and {len(jobs)}")
    selected = {
        JobConfigs.coverage_build_jobs[1].name,
        *(jobs[shard - 1].name for shard in selected_shards),
    }
    return job_name not in selected, "Outside the requested performance coverage shards"


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
        Workflow.Config.InputConfig(
            name="performance_shards",
            description="Performance shards for suite=performance, either all or comma-separated numbers (e.g. 4,13)",
            is_required=False,
            default_value="all",
        ),
        Workflow.Config.InputConfig(
            name="performance_resume_from",
            description="Reuse completed whole tests from this workflow commit SHA on the same branch",
            is_required=False,
            default_value="",
        ),
        Workflow.Config.InputConfig(
            name="performance_max_hours",
            description="Performance collection job budget including export reserve",
            is_required=False,
            default_value="12",
            options=["6", "12"],
        ),
        Workflow.Config.InputConfig(
            name="performance_binary_url",
            description="Immutable coverage binary artifact URL required for recovery",
            is_required=False,
            default_value="",
        ),
        Workflow.Config.InputConfig(
            name="performance_binary_sha256",
            description="SHA256 of the unpacked pinned binary required for recovery",
            is_required=False,
            default_value="",
        ),
    ],
    workflow_filter_hooks=[filter_coverage_jobs],
)

WORKFLOWS = [
    workflow,
]
