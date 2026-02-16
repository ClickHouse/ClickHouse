from ci.defs.defs import RunnerLabels
from praktika import Job, Workflow

workflow = Workflow.Config(
    name="Build Praktika for PyPy",
    event=Workflow.Event.DISPATCH,
    jobs=[
        Job.Config(
            name="Build Praktika",
            runs_on=RunnerLabels.ARM_LARGE,
            command="python3 ./ci/jobs/build_praktika.py",
        ),
    ],
)

WORKFLOWS = [
    workflow,
]
