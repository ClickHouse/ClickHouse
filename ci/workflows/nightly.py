from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    GH_AUTH_TRUSTED_LAMBDA_NAME,
    SECRETS,
    RunnerLabels,
)

# General nightly maintenance workflow. Hosts independent housekeeping jobs that
# run once a day against master:
#   - Prepare changelog: prepares CHANGELOG.md for the upcoming release
#     (see ci/jobs/changelog_nightly.py).
#   - Label external contributors: labels issues and pull requests from authors
#     outside the organization (see ci/jobs/label_external_contributors.py).
# Add further nightly jobs here rather than creating a new workflow each time.

workflow = Workflow.Config(
    name="Nightly",
    engine=Workflow.Engine.GH_ACTIONS,
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    gh_auth_lambda_name=GH_AUTH_TRUSTED_LAMBDA_NAME,
    jobs=[
        Job.Config(
            name="Prepare changelog",
            command="python3 ./ci/jobs/changelog_nightly.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        ),
        Job.Config(
            name="Label external contributors",
            command="python3 ./ci/jobs/label_external_contributors.py",
            runs_on=RunnerLabels.ARM_TINY,
            enable_gh_auth=True,
        ),
    ],
    secrets=SECRETS,
    enable_report=True,
    enable_cidb=False,
    cron_schedules=["23 2 * * *"],
)

WORKFLOWS = [
    workflow,
]
