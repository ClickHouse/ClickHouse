from praktika import Workflow

from ci.defs.defs import (
    DOCKERS,
    GH_AUTH_TRUSTED_LAMBDA_NAME,
    SECRETS,
    ArtifactConfigs,
    ArtifactNames,
)
from ci.defs.job_configs import JobConfigs
from ci.jobs.scripts.workflow_hooks.filter_job import should_skip_merge_queue_job

workflow = Workflow.Config(
    name="MergeQueueCI",
    event=Workflow.Event.MERGE_QUEUE,
    engine=Workflow.Engine.GH_ACTIONS,
    jobs=[
        JobConfigs.style_check,
        JobConfigs.fast_test,
        *[job for job in JobConfigs.build_jobs if job.name == "Build (amd_binary)"],
        # Reruns the PR's new/changed stateless tests against the merge group
        # state, catching semantic conflicts with `master` changes that landed
        # after the PR's last CI run (e.g. a new randomized setting in
        # `tests/clickhouse-test`). Also reruns the tests that `master` added or
        # changed after the PR's last CI run, which the PR may break (see
        # `Targeting.get_merge_queue_drift_tests`). Self-skips when there are no
        # such tests.
        *JobConfigs.stateless_tests_flaky_mq_jobs,
        # Rerun the targeted selection of the PR (its changed tests and the tests
        # covering its changed lines) against the merge group state. A PR tested
        # against an older `master` can break tests that landed after its last CI
        # run, e.g. https://github.com/ClickHouse/ClickHouse/pull/117198 broke two
        # `test_storage_delta` tests added to `master` meanwhile, and
        # https://github.com/ClickHouse/ClickHouse/pull/123832 merged an
        # integration test already broken by a `master` change. The changed
        # stateless tests and those `master` changed after the PR's last CI run
        # are left to the flaky check above; the integration job reruns the
        # integration test modules `master` changed (see
        # `Targeting.get_merge_queue_drift_tests`). The queue is meant to fail
        # fast: both jobs stop at 3 failed tests, and at 30 minutes from their
        # start without failing.
        *JobConfigs.stateless_tests_targeted_mq_jobs,
        *JobConfigs.integration_test_targeted_mq_jobs,
        # Reruns the documentation examples against the merge group state, so a
        # pull request and a `master` commit that are each green on their own
        # cannot break them together. Cheap enough to run unconditionally - see
        # `JobConfigs.docs_examples_mq_job`.
        JobConfigs.docs_examples_mq_job,
        # clang-tidy on the merge group state. The full tidy build takes hours,
        # so the queue cannot run it, and clang-tidy breakage keeps reaching
        # `master` from pull requests that were green before the changes they are
        # merged on top of. This job analyzes only the translation units the pull
        # request touches, which is cheap enough for the queue; the full check on
        # `master` still covers the rest of the tree.
        JobConfigs.tidy_changed_files_job,
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
    enable_job_filtering_by_changes=True,
    workflow_filter_hooks=[should_skip_merge_queue_job],
    pre_hooks=[
        "python3 ./ci/jobs/scripts/workflow_hooks/store_data.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/set_dummy_sync_commit_status.py",
        "python3 ./ci/jobs/scripts/workflow_hooks/check_sync_pr_mergeable.py",
    ],
    # check_sync_pr_mergeable.py needs a token with a broader permission scope -
    # mint it from the dedicated lambda.
    gh_auth_lambda_name=GH_AUTH_TRUSTED_LAMBDA_NAME,
)

WORKFLOWS = [
    workflow,
]
