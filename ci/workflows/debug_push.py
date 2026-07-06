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
    # At least one input is required: the praktika generator always emits an
    # `inputs:` key for dispatch/call, and GitHub rejects it when empty.
    inputs=[
        Workflow.Config.InputConfig(
            name="repo",
            description="Target repository for the probe push (default: this repo)",
            is_required=False,
            default_value="",
        ),
        Workflow.Config.InputConfig(
            name="probe-branch",
            description="Throwaway branch to create and delete for the probe",
            is_required=False,
            default_value="robot-clickhouse/debug-push-probe",
        ),
    ],
)

WORKFLOWS = [workflow]
