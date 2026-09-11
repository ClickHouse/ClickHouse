from praktika import Job, Workflow

from ci.defs.defs import (
    BASE_BRANCH,
    GH_AUTH_TRUSTED_LAMBDA_NAME,
    SECRETS,
    RunnerLabels,
)

# Daily preparation of CHANGELOG.md for the upcoming release. The job
# generates raw changelog entries for the pull requests newly merged into
# master (utils/changelog wrapper around tests/ci/changelog.py) and edits them
# following .claude/skills/edit-changelog/SKILL.md, committing the two states
# separately to a per-release bot branch (auto/changelog-X.Y) with a draft PR
# that the release manager finalizes at release time.
# See ci/jobs/changelog_nightly.py.

workflow = Workflow.Config(
    name="NightlyChangelog",
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
