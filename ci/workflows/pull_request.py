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
    CH_AMD_DEBUG = "CH_AMD_DEBUG"
    CH_AMD_RELEASE = "CH_AMD_RELEASE"


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

amd_build_jobs = Job.Config(
    name=JobNames.BUILD,
    runs_on=[RunnerLabels.BUILDER],
    command="python3 ./ci/jobs/build_clickhouse.py",
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
).parametrize(
    parameter=["amd_debug", "amd_release"],
    provides=[[ArtifactNames.CH_AMD_DEBUG], [ArtifactNames.CH_AMD_RELEASE]],
)

statless_batch_num = 2
stateless_tests_amd_debug_jobs = Job.Config(
    name=JobNames.STATELESS_TESTS,
    runs_on=[RunnerLabels.BUILDER],
    command="python3 ./ci/jobs/functional_stateless_tests.py amd_debug",
    run_in_docker="clickhouse/stateless-test",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_stateless_tests.py",
        ],
    ),
    requires=[ArtifactNames.CH_AMD_DEBUG],
).parametrize(
    parameter=[
        f"parallel {i+1}/{statless_batch_num}" for i in range(statless_batch_num)
    ]
    + ["non-parallel"],
    runs_on=[[RunnerLabels.BUILDER] for _ in range(statless_batch_num)]
    + [[RunnerLabels.STYLE_CHECKER]],
)

workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        style_check_job,
        fast_test_job,
        *amd_build_jobs,
        *stateless_tests_amd_debug_jobs,
    ],
    artifacts=[
        Artifact.Config(
            name=ArtifactNames.CH_AMD_DEBUG,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/build/programs/clickhouse",
        ),
        Artifact.Config(
            name=ArtifactNames.CH_AMD_RELEASE,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/build/programs/clickhouse",
        ),
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


# if __name__ == "__main__":
#     # local job test inside praktika environment
#     from praktika.runner import Runner
#     from praktika.digest import Digest
#
#     print(Digest().calc_job_digest(amd_debug_build_job))
#
#     Runner().run(workflow, fast_test_job, docker="fasttest", local_run=True)
