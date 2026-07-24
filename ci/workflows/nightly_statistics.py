from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
    BuildTypes,
    JobNames,
    RunnerLabels,
)
from ci.defs.job_configs import common_build_job_config, common_ft_job_config

# TODO: add alert on workflow failure

# TODO: make it native praktika workflow + native praktika job - generated automatically if statistics feature is enabled for any workflow

# Preliminary query-metrics collection: build a plain `arm_binary` ClickHouse and
# run the stateless suite against it. The `collect metrics` option makes
# `functional_tests.py` dump the `system.*_log` tables and attach them to the
# job result even on a passing run. It is a single parallel job for now - the
# collected data set is expected to grow in follow-up changes.
collect_query_metrics_build_job = common_build_job_config.parametrize(
    Job.ParamSet(
        parameter=BuildTypes.ARM_BINARY,
        provides=[ArtifactNames.CH_ARM_BINARY],
        runs_on=RunnerLabels.ARM_LARGE,
    ),
)[0]
collect_query_metrics_test_jobs = common_ft_job_config.set_name(
    JobNames.COLLECT_QUERY_METRICS
).set_post_hooks(
    ["python3 ./ci/jobs/scripts/job_hooks/upload_query_metrics_hook.py"]
).parametrize(
    # No "parallel"/"sequential" flavor: a single job runs the whole stateless
    # suite (both parallel and sequential tests) so the collected metrics cover
    # every test in one place.
    Job.ParamSet(
        parameter="arm_binary, collect metrics",
        runs_on=RunnerLabels.ARM_MEDIUM,
        requires=[ArtifactNames.CH_ARM_BINARY],
    ),
)

ch_arm_binary_artifact = [
    a for a in ArtifactConfigs.clickhouse_binaries if a.name == ArtifactNames.CH_ARM_BINARY
][0]

workflow = Workflow.Config(
    name="NightlyStatistics",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Collect Job Duration Statistics",
            command="python3 ./ci/jobs/collect_job_duration_statistics.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
        ),
        Job.Config(
            name="Collect Test Duration Statistics",
            command="python3 ./ci/jobs/collect_test_duration_statistics.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
        ),
        collect_query_metrics_build_job,
        *collect_query_metrics_test_jobs,
    ],
    artifacts=[ch_arm_binary_artifact],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["13 5 * * *"],
)
WORKFLOWS = [
    workflow,
]
