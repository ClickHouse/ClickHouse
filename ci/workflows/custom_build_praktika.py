from ci.defs.defs import RunnerLabels, BASE_BRANCH #REMOVEME
from ci.praktika.secret import Secret
from praktika import Job, Workflow

workflow = Workflow.Config(
    name="Build Praktika for PyPI",
    event=Workflow.Event.PULL_REQUEST,  # for debug Workflow.Event.DISPATCH,REMOVEME
    base_branches=[BASE_BRANCH],  # REMOVEME
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
