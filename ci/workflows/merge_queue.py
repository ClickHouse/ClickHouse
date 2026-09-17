from praktika import Workflow

from ci.defs.defs import DOCKERS, SECRETS, ArtifactConfigs, ArtifactNames
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_merge_queue_job

workflow = Workflow.Config(
    name="MergeQueueCI",
    event=Workflow.Event.MERGE_QUEUE,
    jobs=[
        JobConfigs.style_check,
        JobConfigs.fast_test,
        *[job for job in JobConfigs.build_jobs if job.name == "Build (amd_binary)"],
        # Reruns the PR's new/changed stateless tests against the merge group
        # state, catching semantic conflicts with `master` changes that landed
        # after the PR's last CI run (e.g. a new randomized setting in
        # `tests/clickhouse-test`). Self-skips when the PR changes no tests.
        *JobConfigs.stateless_tests_flaky_mq_jobs,
    ],
    artifacts=[
        *[
            a
            for a in ArtifactConfigs.clickhouse_binaries
            if a.name == ArtifactNames.CH_AMD_BINARY
        ],
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    enable_commit_status_on_failure=True,
    # Config-time skip for the stateless flaky check on PRs that change no
    # stateless tests, so non-test PRs do not schedule the runner and restore
    # the binary only for the job to self-skip inside `functional_tests.py`.
    workflow_filter_hooks=[should_skip_merge_queue_job],
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/set_dummy_sync_commit_status.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/check_sync_pr_mergeable.py",
    ],
)

WORKFLOWS = [
    workflow,
]
