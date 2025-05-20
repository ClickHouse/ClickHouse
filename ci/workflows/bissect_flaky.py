from praktika import Job, Secret, Workflow

from ci.defs.defs import DOCKERS, SECRETS, RunnerLabels

# TODO: it's initial workflow config

workflow = Workflow.Config(
    name="BisectFlaky",
    event=Workflow.Event.DISPATCH,
    jobs=[
        Job.Config(
            name="Bisect",
            runs_on=[RunnerLabels.FUNC_TESTER_ARM],
            command="python3 ./ci/jobs/bisect_job.py",
            # many tests expect to see "/var/lib/clickhouse" in various output lines - add mount for now, consider creating this dir in docker file
            run_in_docker="clickhouse/stateless-test+--security-opt seccomp=unconfined+root+--privileged",
        )
    ],
    inputs=[
        Workflow.Config.InputConfig(
            name="sha_from",
            description="Start of commits range",
            is_required=True,
            default_value="auto",
        ),
        Workflow.Config.InputConfig(
            name="sha_to",
            description="End of commits range",
            is_required=True,
            default_value="auto",
        ),
        Workflow.Config.InputConfig(
            name="test_names",
            description="List of flaky tests",
            is_required=True,
            default_value="auto",
        ),
    ],
    secrets=SECRETS,
    dockers=DOCKERS,
)

WORKFLOWS = [
    workflow,
]
