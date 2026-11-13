"""One-off publication branch, never merge: normal master workflow is unchanged."""

from praktika import Job, Workflow

from ci.defs.defs import RunnerLabels

WORKFLOWS = [
    Workflow.Config(
        name="NightlyCoverage",
        event=Workflow.Event.DISPATCH,
        engine=Workflow.Engine.GH_ACTIONS,
        jobs=[
            Job.Config(
                name="Publish temporary coverage backfill",
                runs_on=RunnerLabels.AMD_TINY,
                command="python3 ./ci/jobs/manual_coverage_backfill_publish.py",
                timeout=600,
                digest_config=None,
            ),
        ],
        enable_cache=False,
        enable_report=True,
        enable_cidb=False,
        enable_slack_feed=False,
    ),
]
