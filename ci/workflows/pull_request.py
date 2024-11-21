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
    CH_ARM_RELEASE = "CH_ARM_RELEASE"
    CH_ARM_ASAN = "CH_ARM_ASAN"

    CH_ODBC_B_AMD_DEBUG = "CH_ODBC_B_AMD_DEBUG"
    CH_ODBC_B_AMD_RELEASE = "CH_ODBC_B_AMD_RELEASE"
    CH_ODBC_B_ARM_RELEASE = "CH_ODBC_B_ARM_RELEASE"
    CH_ODBC_B_ARM_ASAN = "CH_ODBC_B_ARM_ASAN"

    DEB_AMD_DEBUG = "DEB_AMD_DEBUG"
    DEB_AMD_RELEASE = "DEB_AMD_RELEASE"
    DEB_ARM_RELEASE = "DEB_ARM_RELEASE"
    DEB_ARM_ASAN = "DEB_ARM_ASAN"


style_check_job = Job.Config(
    name=JobNames.STYLE_CHECK,
    runs_on=[RunnerLabels.CI_SERVICES],
    command="python3 ./ci/jobs/check_style.py",
    run_in_docker="clickhouse/style-test",
)

fast_test_job = Job.Config(
    name=JobNames.FAST_TEST,
    runs_on=[RunnerLabels.BUILDER_AMD],
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

build_jobs = Job.Config(
    name=JobNames.BUILD,
    runs_on=["...from params..."],
    requires=[],
    command="python3 ./ci/jobs/build_clickhouse.py --build-type {PARAMETER}",
    run_in_docker="clickhouse/fasttest",
    timeout=3600 * 2,
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
            "./ci/jobs/build_clickhouse.py",
        ],
    ),
).parametrize(
    parameter=["amd_debug", "amd_release", "arm_release", "arm_asan"],
    provides=[
        [
            ArtifactNames.CH_AMD_DEBUG,
            ArtifactNames.DEB_AMD_DEBUG,
            ArtifactNames.CH_ODBC_B_AMD_DEBUG,
        ],
        [
            ArtifactNames.CH_AMD_RELEASE,
            ArtifactNames.DEB_AMD_RELEASE,
            ArtifactNames.CH_ODBC_B_AMD_RELEASE,
        ],
        [
            ArtifactNames.CH_ARM_RELEASE,
            ArtifactNames.DEB_ARM_RELEASE,
            ArtifactNames.CH_ODBC_B_ARM_RELEASE,
        ],
        [
            ArtifactNames.CH_ARM_ASAN,
            ArtifactNames.DEB_ARM_ASAN,
            ArtifactNames.CH_ODBC_B_ARM_ASAN,
        ],
    ],
    runs_on=[
        [RunnerLabels.BUILDER_AMD],
        [RunnerLabels.BUILDER_AMD],
        [RunnerLabels.BUILDER_ARM],
        [RunnerLabels.BUILDER_ARM],
    ],
)

stateless_tests_jobs = Job.Config(
    name=JobNames.STATELESS,
    runs_on=[RunnerLabels.BUILDER_AMD],
    command="python3 ./ci/jobs/functional_stateless_tests.py --test-options {PARAMETER}",
    # many tests expect to see "/var/lib/clickhouse" in various output lines - add mount for now, consider creating this dir in docker file
    run_in_docker="clickhouse/stateless-test+--security-opt seccomp=unconfined",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_stateless_tests.py",
        ],
    ),
).parametrize(
    parameter=[
        "amd_debug,parallel",
        "amd_debug,non-parallel",
        "amd_release,parallel",
        "amd_release,non-parallel",
        "arm_asan,parallel",
        "arm_asan,non-parallel",
    ],
    runs_on=[
        [RunnerLabels.BUILDER_AMD],
        [RunnerLabels.FUNC_TESTER_AMD],
        [RunnerLabels.BUILDER_AMD],
        [RunnerLabels.FUNC_TESTER_AMD],
        [RunnerLabels.BUILDER_ARM],
        [RunnerLabels.FUNC_TESTER_ARM],
    ],
    requires=[
        [ArtifactNames.CH_AMD_DEBUG, ArtifactNames.CH_ODBC_B_AMD_DEBUG],
        [ArtifactNames.CH_AMD_DEBUG, ArtifactNames.CH_ODBC_B_AMD_DEBUG],
        [ArtifactNames.CH_AMD_RELEASE, ArtifactNames.CH_ODBC_B_AMD_RELEASE],
        [ArtifactNames.CH_AMD_RELEASE, ArtifactNames.CH_ODBC_B_AMD_RELEASE],
        [ArtifactNames.CH_ARM_ASAN, ArtifactNames.CH_ODBC_B_ARM_ASAN],
        [ArtifactNames.CH_ARM_ASAN, ArtifactNames.CH_ODBC_B_ARM_ASAN],
    ],
)

stateful_tests_jobs = Job.Config(
    name=JobNames.STATEFUL,
    runs_on=[RunnerLabels.BUILDER_AMD],
    command="python3 ./ci/jobs/functional_stateful_tests.py --test-options {PARAMETER}",
    # many tests expect to see "/var/lib/clickhouse"
    # some tests expect to see "/var/log/clickhouse"
    run_in_docker="clickhouse/stateless-test+--security-opt seccomp=unconfined",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_stateful_tests.py",
        ],
    ),
).parametrize(
    parameter=[
        "amd_release,parallel",
    ],
    runs_on=[
        [RunnerLabels.BUILDER_AMD],
    ],
    requires=[
        [ArtifactNames.CH_AMD_DEBUG],
    ],
)

# TODO: refactor job to be aligned with praktika style (remove wrappers, run in docker)
stress_test_jobs = Job.Config(
    name=JobNames.STRESS,
    runs_on=[RunnerLabels.BUILDER_ARM],
    command="python3 ./tests/ci/stress_check.py {PARAMETER}",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_stateful_tests.py",
        ],
    ),
).parametrize(
    parameter=[
        "arm_release",
    ],
    runs_on=[
        [RunnerLabels.FUNC_TESTER_ARM],
    ],
    requires=[
        [ArtifactNames.DEB_ARM_RELEASE],
    ],
)


workflow = Workflow.Config(
    name="PR",
    event=Workflow.Event.PULL_REQUEST,
    base_branches=[BASE_BRANCH],
    jobs=[
        style_check_job,
        fast_test_job,
        *build_jobs,
        *stateless_tests_jobs,
        *stateful_tests_jobs,
        *stress_test_jobs,
    ],
    artifacts=[
        *Artifact.Config(
            name="...",
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/build/programs/clickhouse",
        ).parametrize(
            names=[
                ArtifactNames.CH_AMD_DEBUG,
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.CH_ARM_ASAN,
            ]
        ),
        *Artifact.Config(
            name="...",
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/build/programs/clickhouse-odbc-bridge",
        ).parametrize(
            names=[
                ArtifactNames.CH_ODBC_B_AMD_DEBUG,
                ArtifactNames.CH_ODBC_B_AMD_RELEASE,
                ArtifactNames.CH_ODBC_B_ARM_RELEASE,
                ArtifactNames.CH_ODBC_B_ARM_ASAN,
            ]
        ),
        Artifact.Config(
            name=ArtifactNames.DEB_AMD_DEBUG,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/output/*.deb",
        ),
        Artifact.Config(
            name=ArtifactNames.DEB_AMD_RELEASE,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/output/*.deb",
        ),
        Artifact.Config(
            name=ArtifactNames.DEB_ARM_RELEASE,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/output/*.deb",
        ),
        Artifact.Config(
            name=ArtifactNames.DEB_ARM_ASAN,
            type=Artifact.Type.S3,
            path=f"{Settings.TEMP_DIR}/output/*.deb",
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
]


# if __name__ == "__main__":
#     # local job test inside praktika environment
#     from praktika.runner import Runner
#     from praktika.digest import Digest
#
#     print(Digest().calc_job_digest(amd_debug_build_job))
#
#     Runner().run(workflow, fast_test_job, docker="fasttest", local_run=True)
