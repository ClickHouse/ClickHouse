from typing import List

from praktika import Artifact, Job, Workflow
from praktika.settings import Settings

from ci.settings.definitions import (
    BASE_BRANCH,
    DOCKERS,
    SECRETS,
    JobNames,
    RunnerLabels,
)


class ArtifactNames:
    ch_debug_binary = "clickhouse_debug_binary"


style_check_job = Job.Config(
    name=JobNames.STYLE_CHECK,
    runs_on=[RunnerLabels.CI_SERVICES],
    command="python3 ./ci/jobs/check_style.py",
    run_in_docker="clickhouse/style-test",
)

fast_test_job = Job.Config(
    name=JobNames.FAST_TEST,
    runs_on=[RunnerLabels.BUILDER],
    command="python3 ./ci/jobs/fast_test.py",
    run_in_docker="clickhouse/fasttest",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/fast_test.py",
            "./tests/queries/0_stateless/",
            "./src",
        ],
    ),
)

job_build_amd_debug = Job.Config(
    name=JobNames.BUILD_AMD_DEBUG,
    runs_on=[RunnerLabels.BUILDER],
    command="python3 ./ci/jobs/build_clickhouse.py amd_debug",
    run_in_docker="clickhouse/fasttest",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./src",
            "./contrib/",
            "./CMakeLists.txt",
            "./PreLoad.cmake",
            "./cmake",
            "./base",
            "./programs",
            "./docker/packager/packager",
            "./rust",
            "./tests/ci/version_helper.py",
        ],
    ),
    provides=[ArtifactNames.ch_debug_binary],
)

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        style_check_job,
        fast_test_job,
        job_build_amd_debug,
    ],
    artifacts=[
        Artifact.Config(
            name=ArtifactNames.ch_debug_binary,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/build/programs/clickhouse",
        )
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_merge_ready_status=True,
)

WORKFLOWS = [
    workflow,
]  # type: List[Workflow.Config]


if __name__ == "__main__":
    # local job test inside praktika environment
    from praktika.runner import Runner

    Runner().run(workflow, fast_test_job, docker="fasttest", dummy_env=True)
