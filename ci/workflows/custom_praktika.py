from ci.settings.settings import RunnerLabels
from praktika import Job, Secret, Workflow

workflow = Workflow.Config(
    name="Build Praktika for PyPy",
    event=Workflow.Event.DISPATCH,
    jobs=[
        Job.Config(
    name="Build Praktika",
    runs_on=RunnerLabels.ARM_LARGE,
    command="""
        set -e
        sudo apt install pypy3 pypy3-dev pypy3-venv
        source .venv-pypy/bin/activate
        pip install --upgrade pip setuptools wheel
""",
        ),
    ],
)

WORKFLOWS = [
    workflow,
]
