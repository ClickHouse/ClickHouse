from praktika import Job, Workflow

from ci.defs.defs import SECRETS

debug_push_job = Job.Config(
    name="DebugPush",
    runs_on=["self-hosted", "amd-release-maker"],
    command="PYTHONPATH=. python3 ./ci/jobs/debug_push_job.py",
    timeout=1800,
    # Same App auth as CreateRelease: `gh` (and git, via `gh auth setup-git`)
    # authenticate as the `clickhouse-gh` App, so the probe push reproduces the
    # release's exact auth from the checked-out tree.
    enable_gh_auth=True,
)

workflow = Workflow.Config(
    name="DebugPush",
    event=Workflow.Event.DISPATCH,
    jobs=[debug_push_job],
    secrets=SECRETS,
)

WORKFLOWS = [workflow]
