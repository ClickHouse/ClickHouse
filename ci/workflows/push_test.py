from praktika import Workflow

from ci.workflows.defs import ARTIFACTS, BASE_BRANCH, DOCKERS, SECRETS, Jobs

S3_BUILDS_BUCKET = "clickhouse-builds"

workflow = Workflow.Config(
    name="Test Master",
    event=Workflow.Event.PUSH,
    branches=["ci_test_*"],
    jobs=[
        *Jobs.build_jobs,
    ],
    artifacts=ARTIFACTS,
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
)

WORKFLOWS = [
    workflow,
]
