from praktika import Workflow

from ci.workflows.defs import ARTIFACTS, BASE_BRANCH, DOCKERS, SECRETS, Jobs, LegacyJobs

S3_BUILDS_BUCKET = "clickhouse-builds"

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        LegacyJobs.style_check,
        LegacyJobs.fast_test,
    ],
    # artifacts=ARTIFACTS,
    # dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    enable_merge_ready_status=True,
    pre_hooks=[
        "python3 ./ci/jobs/scripts/prechecks/pr_description.py",
        "python3 ./ci/jobs/scripts/prechecks/trusted.py",
        "python3 ./ci/jobs/scripts/prechecks/docker_digests.py",
    ],
    post_hooks=[],
)

WORKFLOWS = [
    workflow,
]
