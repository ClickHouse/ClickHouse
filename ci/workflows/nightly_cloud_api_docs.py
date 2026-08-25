from praktika import Job, Workflow

from ci.defs.defs import BASE_BRANCH, SECRETS, RunnerLabels

# Nightly sync of the ClickHouse Cloud API reference docs from the live OpenAPI
# spec (https://api.clickhouse.cloud/v1). The job regenerates the per-endpoint
# pages + docs.json navigation so each endpoint carries its own maturity badge
# (Beta / Private preview / Deprecated), and opens or refreshes a single bot PR
# when the docs drift from the spec. See ci/jobs/cloud_api_docs_nightly.py.

workflow = Workflow.Config(
    name="NightlyCloudAPIDocs",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        Job.Config(
            name="Sync Cloud API docs",
            command="python3 ./ci/jobs/cloud_api_docs_nightly.py",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["13 7 * * *"],
)

WORKFLOWS = [
    workflow,
]
