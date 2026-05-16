from ci.defs.defs import RunnerLabels
from ci.praktika.secret import Secret
from praktika import Job, Workflow

workflow = Workflow.Config(
    name="Build Praktika for PyPI",
    event=Workflow.Event.DISPATCH,
    jobs=[
        Job.Config(
            name="Build Praktika",
            runs_on=RunnerLabels.ARM_LARGE,
            command="python3 ./ci/jobs/build_praktika.py",
        ),
    ],
    secrets=[
        Secret.Config(
            name="TWINE_PASSWORD",
            type=Secret.Type.GH_SECRET,
        ),
    ],
)

WORKFLOWS = [
    workflow,
]
