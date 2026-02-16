from ci.defs.defs import RunnerLabels, BASE_BRANCH
from praktika import Job, Workflow

workflow = Workflow.Config(
    name="Build Praktika for PyPy",
    event=Workflow.Event.PULL_REQUEST, # for debug Workflow.Event.DISPATCH,
    base_branches=[BASE_BRANCH], # REMOVEME
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
