from praktika import Artifact, Docker, Job, Secret
from praktika.utils import Utils

TEMP_DIR = f"{Utils.cwd()}/ci/tmp"  # == SettingsTEMP_DIR


class RunnerLabels:
    CI_SERVICES = "ci_services"
    CI_SERVICES_EBS = "ci_services_ebs"
    BUILDER_AMD = "builder"
    BUILDER_ARM = "builder-aarch64"
    FUNC_TESTER_AMD = "func-tester"
    FUNC_TESTER_ARM = "func-tester-aarch64"
    STYLE_CHECK_AMD = "style-checker"
    STYLE_CHECK_ARM = "style-checker-aarch64"


class CIFiles:
    UNIT_TESTS_RESULTS = f"{TEMP_DIR}/unit_tests_result.json"
    UNIT_TESTS_BIN = f"{TEMP_DIR}/build/src/unit_tests_dbms"


BASE_BRANCH = "master"

azure_secret = Secret.Config(
    name="azure_connection_string",
    type=Secret.Type.AWS_SSM_VAR,
)

SECRETS = [
    Secret.Config(
        name="dockerhub_robot_password",
        type=Secret.Type.AWS_SSM_VAR,
    ),
    azure_secret,
    # Secret.Config(
    #     name="woolenwolf_gh_app.clickhouse-app-id",
    #     type=Secret.Type.AWS_SSM_SECRET,
    # ),
    # Secret.Config(
    #     name="woolenwolf_gh_app.clickhouse-app-key",
    #     type=Secret.Type.AWS_SSM_SECRET,
    # ),
]

DOCKERS = [
    Docker.Config(
        name="clickhouse/binary-builder",
        path="./ci/docker/binary-builder",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/fasttest"],
    ),
    # Docker.Config(
    #     name="clickhouse/cctools",
    #     path="./ci/docker/packager/cctools",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="clickhouse/test-old-centos",
        path="./ci/docker/compatibility/centos",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/test-old-ubuntu",
        path="./ci/docker/compatibility/ubuntu",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #     name="clickhouse/test-util",
    #     path="./ci/docker/test/util",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-test",
    #     path="./ci/docker/test/integration/base",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/fuzzer",
    #     path="./ci/docker/test/fuzzer",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/performance-comparison",
    #     path="./ci/docker/test/performance-comparison",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="clickhouse/fasttest",
        path="./ci/docker/fasttest",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #     name="clickhouse/keeper-jepsen-test",
    #     path="./ci/docker/test/keeper-jepsen",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/server-jepsen-test",
    #     path="./ci/docker/test/server-jepsen",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/sqllogic-test",
    #     path="./ci/docker/test/sqllogic",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    Docker.Config(
        name="clickhouse/stateless-test",
        path="./ci/docker/stateless-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/stateful-test",
        path="./ci/docker/stateful-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/stateless-test"],
    ),
    # Docker.Config(
    #     name="clickhouse/stress-test",
    #     path="./ci/docker/test/stress",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/stateful-test"],
    # ),
    # Docker.Config(
    #     name="clickhouse/unit-test",
    #     path="./ci/docker/test/unit",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-tests-runner",
    #     path="./ci/docker/test/integration/runner",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    Docker.Config(
        name="clickhouse/style-test",
        path="./ci/docker/style-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/docs-builder",
        path="./docker/docs/builder",
        platforms=[Docker.Platforms.ARM],
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/install-deb-test",
        path="docker/test/install/deb",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/install-rpm-test",
        path="docker/test/install/rpm",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/sqlancer-test",
        path="./ci/docker/sqlancer-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
]

# TODO:
# "docker/test/integration/s3_proxy": {
#     "name": "clickhouse/s3-proxy",
#     "dependent": []
# },
# "docker/test/integration/resolver": {
#     "name": "clickhouse/python-bottle",
#     "dependent": []
# },
# "docker/test/integration/helper_container": {
#     "name": "clickhouse/integration-helper",
#     "dependent": []
# },
# "docker/test/integration/mysql_golang_client": {
#     "name": "clickhouse/mysql-golang-client",
#     "dependent": []
# },
# "docker/test/integration/dotnet_client": {
#     "name": "clickhouse/dotnet-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_java_client": {
#     "name": "clickhouse/mysql-java-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_js_client": {
#     "name": "clickhouse/mysql-js-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_php_client": {
#     "name": "clickhouse/mysql-php-client",
#     "dependent": []
# },
# "docker/test/integration/postgresql_java_client": {
#     "name": "clickhouse/postgresql-java-client",
#     "dependent": []
# },
# "docker/test/integration/kerberos_kdc": {
#     "only_amd64": true,
#     "name": "clickhouse/kerberos-kdc",
#     "dependent": []
# },
# "docker/test/integration/nginx_dav": {
#     "name": "clickhouse/nginx-dav",
#     "dependent": []
# }


class BuildTypes:
    AMD_DEBUG = "amd_debug"
    AMD_RELEASE = "amd_release"
    AMD_BINARY = "amd_binary"
    AMD_ASAN = "amd_asan"
    AMD_TSAN = "amd_tsan"
    AMD_MSAN = "amd_msan"
    AMD_UBSAN = "amd_ubsan"
    ARM_RELEASE = "arm_release"
    ARM_ASAN = "arm_asan"


class JobNames:
    STYLE_CHECK = "Style Check"
    FAST_TEST = "Fast test"
    BUILD = "Build"
    STATELESS = "Stateless tests"
    STATEFUL = "Stateful tests"
    STRESS = "Stress tests"
    UPGRADE = "Upgrade tests"
    PERFORMANCE = "Performance comparison"
    COMPATIBILITY = "Compatibility check"
    Docs = "Docs check"
    CLICKBENCH = "ClickBench"
    DOCKER_SERVER = "Docker server"
    SQL_TEST = "SQLTest"
    SQLANCER = "SQLancer"
    INSTALL_CHECK = "Install check"


class ToolSet:
    COMPILER_C = "clang-19"
    COMPILER_CPP = "clang++-19"


class ArtifactNames:
    CH_AMD_DEBUG = "CH_AMD_DEBUG"
    CH_AMD_RELEASE = "CH_AMD_RELEASE"
    CH_AMD_ASAN = "CH_AMD_ASAN"
    CH_AMD_TSAN = "CH_AMD_TSAN"
    CH_AMD_MSAN = "CH_AMD_MSAN"
    CH_AMD_UBSAN = "CH_AMD_UBSAN"
    CH_AMD_BINARY = "CH_AMD_BINARY"
    CH_ARM_RELEASE = "CH_ARM_RELEASE"
    CH_ARM_ASAN = "CH_ARM_ASAN"

    CH_ODBC_B_AMD_DEBUG = "CH_ODBC_B_AMD_DEBUG"
    CH_ODBC_B_AMD_RELEASE = "CH_ODBC_B_AMD_RELEASE"
    CH_ODBC_B_AMD_ASAN = "CH_ODBC_B_AMD_ASAN"
    CH_ODBC_B_AMD_TSAN = "CH_ODBC_B_AMD_TSAN"
    CH_ODBC_B_AMD_MSAN = "CH_ODBC_B_AMD_MSAN"
    CH_ODBC_B_AMD_UBSAN = "CH_ODBC_B_AMD_UBSAN"
    CH_ODBC_B_ARM_RELEASE = "CH_ODBC_B_ARM_RELEASE"
    CH_ODBC_B_ARM_ASAN = "CH_ODBC_B_ARM_ASAN"

    UNITTEST_AMD_ASAN = "UNITTEST_AMD_ASAN"
    UNITTEST_AMD_TSAN = "UNITTEST_AMD_TSAN"
    UNITTEST_AMD_MSAN = "UNITTEST_AMD_MSAN"
    UNITTEST_AMD_UBSAN = "UNITTEST_AMD_UBSAN"
    UNITTEST_AMD_BINARY = "UNITTEST_AMD_BINARY"

    DEB_AMD_DEBUG = "DEB_AMD_DEBUG"
    DEB_AMD_RELEASE = "DEB_AMD_RELEASE"
    DEB_AMD_ASAN = "DEB_AMD_ASAN"
    DEB_AMD_TSAN = "DEB_AMD_TSAN"
    DEB_AMD_MSAM = "DEB_AMD_MSAM"
    DEB_AMD_UBSAN = "DEB_AMD_UBSAN"
    DEB_ARM_RELEASE = "DEB_ARM_RELEASE"
    DEB_ARM_ASAN = "DEB_ARM_ASAN"

    PERF_REPORTS_AMD_1_3 = "PERF_REPORTS_AMD_1_3"
    PERF_REPORTS_AMD_2_3 = "PERF_REPORTS_AMD_2_3"
    PERF_REPORTS_AMD_1_3_WITH_RELEASE = "PERF_REPORTS_AMD_1_3_WITH_RELEASE"
    PERF_REPORTS_AMD_2_3_WITH_RELEASE = "PERF_REPORTS_AMD_2_3_WITH_RELEASE"

    PERF_REPORTS_ARM = "PERF_REPORTS_ARM"


ARTIFACTS = [
    *Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/clickhouse",
    ).parametrize(
        names=[
            ArtifactNames.CH_AMD_DEBUG,
            ArtifactNames.CH_AMD_RELEASE,
            ArtifactNames.CH_AMD_ASAN,
            ArtifactNames.CH_AMD_TSAN,
            ArtifactNames.CH_AMD_MSAN,
            ArtifactNames.CH_AMD_UBSAN,
            ArtifactNames.CH_AMD_BINARY,
            ArtifactNames.CH_ARM_RELEASE,
            ArtifactNames.CH_ARM_ASAN,
        ]
    ),
    *Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/clickhouse-odbc-bridge",
    ).parametrize(
        names=[
            ArtifactNames.CH_ODBC_B_AMD_DEBUG,
            ArtifactNames.CH_ODBC_B_AMD_ASAN,
            ArtifactNames.CH_ODBC_B_AMD_TSAN,
            ArtifactNames.CH_ODBC_B_AMD_MSAN,
            ArtifactNames.CH_ODBC_B_AMD_UBSAN,
            ArtifactNames.CH_ODBC_B_AMD_RELEASE,
            ArtifactNames.CH_ODBC_B_ARM_RELEASE,
            ArtifactNames.CH_ODBC_B_ARM_ASAN,
        ]
    ),
    *Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.deb",
    ).parametrize(
        names=[
            ArtifactNames.DEB_AMD_DEBUG,
            ArtifactNames.DEB_AMD_ASAN,
            ArtifactNames.DEB_AMD_TSAN,
            ArtifactNames.DEB_AMD_MSAM,
            ArtifactNames.DEB_AMD_UBSAN,
        ]
    ),
    Artifact.Config(
        name=ArtifactNames.DEB_AMD_RELEASE,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.deb",
    ),
    Artifact.Config(
        name=ArtifactNames.DEB_ARM_RELEASE,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.deb",
    ),
    Artifact.Config(
        name=ArtifactNames.DEB_ARM_ASAN,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.deb",
    ),
    *Artifact.Config(
        name="",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/perf_wd/*.html",
    ).parametrize(
        names=[
            ArtifactNames.PERF_REPORTS_AMD_1_3,
            ArtifactNames.PERF_REPORTS_AMD_2_3,
            ArtifactNames.PERF_REPORTS_AMD_1_3_WITH_RELEASE,
            ArtifactNames.PERF_REPORTS_AMD_2_3_WITH_RELEASE,
        ]
    ),
    # Artifact.Config(
    #     name=ArtifactNames.PERF_REPORTS_ARM,
    #     type=Artifact.Type.S3,
    #     path=f"{Settings.TEMP_DIR}/perf_wd/*.html",
    # ),
]


class Jobs:
    style_check_job = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=[RunnerLabels.STYLE_CHECK_ARM],
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
        run_in_docker="clickhouse/binary-builder+--network=host",
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
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_RELEASE,
            BuildTypes.AMD_ASAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_UBSAN,
            BuildTypes.AMD_BINARY,
            BuildTypes.ARM_RELEASE,
            BuildTypes.ARM_ASAN,
        ],
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
                ArtifactNames.CH_AMD_ASAN,
                ArtifactNames.DEB_AMD_ASAN,
                ArtifactNames.CH_ODBC_B_AMD_ASAN,
                # ArtifactNames.UNITTEST_AMD_ASAN,
            ],
            [
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.DEB_AMD_TSAN,
                ArtifactNames.CH_ODBC_B_AMD_TSAN,
                # ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            [
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.DEB_AMD_MSAM,
                ArtifactNames.CH_ODBC_B_AMD_MSAN,
                # ArtifactNames.UNITTEST_AMD_MSAN,
            ],
            [
                ArtifactNames.CH_AMD_UBSAN,
                ArtifactNames.DEB_AMD_UBSAN,
                ArtifactNames.CH_ODBC_B_AMD_UBSAN,
                # ArtifactNames.UNITTEST_AMD_UBSAN,
            ],
            [
                ArtifactNames.CH_AMD_BINARY,
                # ArtifactNames.UNITTEST_AMD_BINARY,
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
            [RunnerLabels.BUILDER_AMD],
            [RunnerLabels.BUILDER_AMD],
            [RunnerLabels.BUILDER_AMD],
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
        run_in_docker="clickhouse/stateless-test+--security-opt seccomp=unconfined",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/functional_stateful_tests.py",
            ],
        ),
    ).parametrize(
        parameter=[
            BuildTypes.ARM_RELEASE,
            BuildTypes.AMD_ASAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_UBSAN,
            BuildTypes.AMD_DEBUG,
        ],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_ARM],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
        ],
        requires=[
            [ArtifactNames.CH_ARM_RELEASE],
            [ArtifactNames.CH_AMD_ASAN],
            [ArtifactNames.CH_AMD_TSAN],
            [ArtifactNames.CH_AMD_MSAN],
            [ArtifactNames.CH_AMD_UBSAN],
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
            BuildTypes.ARM_RELEASE,
        ],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_ARM],
        ],
        requires=[
            [ArtifactNames.DEB_ARM_RELEASE],
        ],
    )

    # TODO: refactor job to be aligned with praktika style (remove wrappers, run in docker)
    upgrade_test_jobs = Job.Config(
        name=JobNames.UPGRADE,
        runs_on=["from param"],
        command="python3 ./tests/ci/upgrade_check.py {PARAMETER}",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/upgrade_check.py", "./tests/docker_scripts/"]
        ),
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.ARM_ASAN,
        ],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_ARM],
        ],
        requires=[
            [ArtifactNames.DEB_AMD_DEBUG],
            [ArtifactNames.DEB_AMD_TSAN],
            [ArtifactNames.DEB_AMD_MSAM],
            [ArtifactNames.DEB_ARM_ASAN],
        ],
    )

    # UPGRADE_TEST = JobConfig(
    #     job_name_keyword="upgrade",
    #     digest=DigestConfig(
    #         include_paths=["./tests/ci/upgrade_check.py", "./tests/docker_scripts/"],
    #         exclude_files=[".md"],
    #         docker=["clickhouse/stress-test"],
    #     ),
    #     run_command="upgrade_check.py",
    #     runner_type=Runners.FUNC_TESTER,
    #     timeout=3600,
    # )

    performance_comparison_head_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command="python3 ./ci/jobs/performance_tests.py --test-options {PARAMETER}",
        run_in_docker="clickhouse/stateless-test",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
            ],
        ),
        timeout=2 * 3600,
    ).parametrize(
        parameter=[
            "amd_release,head_master,1/2",
            "amd_release,head_master,2/2",
        ],
        # "arm_release,1/3"],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_AMD]
            for _ in range(2)
            # [RunnerLabels.FUNC_TESTER_ARM],
        ],
        requires=[[ArtifactNames.CH_AMD_RELEASE] for _ in range(2)],
        # [ArtifactNames.CH_ARM_RELEASE]],
        provides=[
            [ArtifactNames.PERF_REPORTS_AMD_1_3],
            [ArtifactNames.PERF_REPORTS_AMD_2_3],
        ],
        # [ArtifactNames.PERF_REPORTS_ARM]],
    )

    performance_comparison_release_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command="python3 ./ci/jobs/performance_tests.py --test-options {PARAMETER}",
        run_in_docker="clickhouse/stateless-test",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
            ],
        ),
        timeout=2 * 3600,
    ).parametrize(
        parameter=[
            "amd_release,prev_release,1/2",
            "amd_release,prev_release,2/2",
        ],
        # "arm_release,1/3"],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_AMD]
            for _ in range(2)
            # [RunnerLabels.FUNC_TESTER_ARM],
        ],
        requires=[[ArtifactNames.CH_AMD_RELEASE] for _ in range(2)],
        # [ArtifactNames.CH_ARM_RELEASE]],
        provides=[
            [ArtifactNames.PERF_REPORTS_AMD_1_3_WITH_RELEASE],
            [ArtifactNames.PERF_REPORTS_AMD_2_3_WITH_RELEASE],
        ],
        # [ArtifactNames.PERF_REPORTS_ARM]],
    )

    compatibility_test_jobs = Job.Config(
        name=JobNames.COMPATIBILITY,
        runs_on=["#from param"],
        command="python3 ./tests/ci/compatibility_check.py --check-name {PARAMETER}",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/compatibility_check.py",
                "./docker/test/compatibility",
            ],
        ),
    ).parametrize(
        parameter=["amd_release", "arm_release"],
        runs_on=[
            [RunnerLabels.STYLE_CHECK_AMD],
            [RunnerLabels.STYLE_CHECK_ARM],
        ],
        requires=[[ArtifactNames.DEB_AMD_RELEASE], [ArtifactNames.DEB_ARM_RELEASE]],
    )
    docs_job = Job.Config(
        name=JobNames.Docs,
        runs_on=[RunnerLabels.FUNC_TESTER_ARM],
        command="python3 ./tests/ci/docs_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=["**/*.md", "./docs", "tests/ci/docs_check.py"],
        ),
    )
    clickbench_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=[RunnerLabels.FUNC_TESTER_AMD],
        command="python3 ./ci/jobs/clickbench.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/clickbench.py", "./ci/jobs/scripts/clickbench/"],
        ),
        run_in_docker="clickhouse/stateless-test",
        timeout=900,
    ).parametrize(
        parameter=[
            BuildTypes.AMD_RELEASE,
            BuildTypes.ARM_RELEASE,
        ],
        runs_on=[
            [RunnerLabels.FUNC_TESTER_AMD],
            [RunnerLabels.FUNC_TESTER_ARM],
        ],
        requires=[
            [ArtifactNames.CH_AMD_RELEASE],
            [ArtifactNames.CH_ARM_RELEASE],
        ],
    )
    # docker_job = Job.Config(
    #     name=JobNames.DOCKER_SERVER,
    #     runs_on=[RunnerLabels.STYLE_CHECK_ARM],
    #     command="python3 ./ci/jobs/docker_server_job.py --from-binary",
    #     digest_config=Job.CacheDigestConfig(
    #         include_paths=[
    #             "./ci/jobs/docker_server_from_binary.py",
    #             "./ci/docker/clickhouse-server",
    #         ],
    #     ),
    #     requires=[ArtifactNames.CH_AMD_RELEASE, ArtifactNames.CH_ARM_RELEASE],
    # )
    docker_job = Job.Config(
        name=JobNames.DOCKER_SERVER,
        # on ARM clickhouse-local call in docker build for amd leads to an error: Instruction check fail. The CPU does not support SSSE3 instruction set
        runs_on=[RunnerLabels.STYLE_CHECK_AMD],
        command="python3 ./ci/jobs/docker_server_job.py --from-deb",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server_job.py",
                "./ci/docker/clickhouse-server",
            ],
        ),
        requires=[ArtifactNames.DEB_AMD_RELEASE, ArtifactNames.DEB_ARM_RELEASE],
    )
    # TODO: make it release only
    sqltest_job = Job.Config(
        name=JobNames.SQL_TEST,
        # on ARM clickhouse-local call in docker build for amd leads to an error: Instruction check fail. The CPU does not support SSSE3 instruction set
        runs_on=[RunnerLabels.FUNC_TESTER_ARM],
        command="python3 ./ci/jobs/sqltest_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqltest_job.py",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    sqlancer_job = Job.Config(
        name=JobNames.SQLANCER,
        runs_on=[RunnerLabels.FUNC_TESTER_ARM],
        command="./ci/jobs/sqlancer_job.sh",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/sqlancer_job.sh"],
        ),
        run_in_docker="clickhouse/sqlancer-test",
        requires=[ArtifactNames.CH_ARM_RELEASE],
        timeout=3600,
    )
    # TODO: add tgz and rpm
    install_check_job = Job.Config(
        name=JobNames.INSTALL_CHECK,
        runs_on=["..."],
        command="python3 ./tests/ci/install_check.py dummy_check_name --no-rpm --no-tgz --no-download",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/install_check.py"],
        ),
        timeout=900,
    ).parametrize(
        parameter=[
            BuildTypes.AMD_RELEASE,
            BuildTypes.ARM_RELEASE,
        ],
        runs_on=[
            [RunnerLabels.STYLE_CHECK_AMD],
            [RunnerLabels.STYLE_CHECK_ARM],
        ],
        requires=[
            [ArtifactNames.DEB_AMD_RELEASE, ArtifactNames.CH_AMD_RELEASE],
            [ArtifactNames.DEB_ARM_RELEASE, ArtifactNames.CH_ARM_RELEASE],
        ],
    )
