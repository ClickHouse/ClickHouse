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
    # Trigger on push to the work branch, not dispatch: `workflow_dispatch` only
    # works when the workflow file is on the default branch, and we must not push
    # this to master. A push-triggered workflow runs the file from the pushed
    # branch itself, so every push to cr-work runs the probe on the release runner.
    event=Workflow.Event.PUSH,
    branches=["cr-work"],
    jobs=[debug_push_job],
    secrets=SECRETS,
)

WORKFLOWS = [workflow]
