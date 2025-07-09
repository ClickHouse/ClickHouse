from praktika import Artifact, Docker, Job, Secret
from praktika.utils import MetaClasses, Utils

# i.e. "ClickHouse/ci/tmp"
TEMP_DIR = f"{Utils.cwd()}/ci/tmp"  # == _Settings.TEMP_DIR != env_helper.TEMP_PATH

SYNC = "CH Inc sync"

S3_BUCKET_NAME = "clickhouse-builds"
S3_REPORT_BUCKET_NAME = "clickhouse-test-reports"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"
S3_REPORT_BUCKET_HTTP_ENDPOINT = "s3.amazonaws.com/clickhouse-test-reports"


class RunnerLabels:
    CI_SERVICES = "ci_services"
    CI_SERVICES_EBS = "ci_services_ebs"
    BUILDER_AMD = ["self-hosted", "builder"]
    BUILDER_ARM = ["self-hosted", "builder-aarch64"]
    FUNC_TESTER_AMD = ["self-hosted", "func-tester"]
    FUNC_TESTER_ARM = ["self-hosted", "func-tester-aarch64"]
    STYLE_CHECK_AMD = ["self-hosted", "style-checker"]
    STYLE_CHECK_ARM = ["self-hosted", "style-checker-aarch64"]


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
    Secret.Config(
        name="clickhouse-test-stat-url",
        type=Secret.Type.AWS_SSM_VAR,
    ),
    Secret.Config(
        name="clickhouse-test-stat-login",
        type=Secret.Type.AWS_SSM_VAR,
    ),
    Secret.Config(
        name="clickhouse-test-stat-password",
        type=Secret.Type.AWS_SSM_VAR,
    ),
    azure_secret,
    Secret.Config(
        name="woolenwolf_gh_app.clickhouse-app-id",
        type=Secret.Type.AWS_SSM_SECRET,
    ),
    Secret.Config(
        name="woolenwolf_gh_app.clickhouse-app-key",
        type=Secret.Type.AWS_SSM_SECRET,
    ),
]

DOCKERS = [
    Docker.Config(
        name="clickhouse/style-test",
        path="./ci/docker/style-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/fasttest",
        path="./ci/docker/fasttest",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/binary-builder",
        path="./ci/docker/binary-builder",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/fasttest"],
    ),
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
    # new images
    # Docker.Config(
    #     name="clickhouse/stateless-test",
    #     path="./ci/docker/stateless-test",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="clickhouse/cctools",
        path="./docker/packager/cctools",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/fasttest"],
    ),
    Docker.Config(
        name="clickhouse/test-util",
        path="./docker/test/util",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/test-base",
        path="./docker/test/base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-util"],
    ),
    Docker.Config(
        name="clickhouse/stateless-test",
        path="./docker/test/stateless",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/stress-test",
        path="./docker/test/stress",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/stateless-test"],
    ),
    Docker.Config(
        name="clickhouse/fuzzer",
        path="./docker/test/fuzzer",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/performance-comparison",
        path="./docker/test/performance-comparison",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/keeper-jepsen-test",
        path="./docker/test/keeper-jepsen",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/server-jepsen-test",
        path="./docker/test/server-jepsen",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/integration-test",
        path="./docker/test/integration/base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    # Docker.Config(
    #     name="clickhouse/integration-test",
    #     path="./ci/docker/integration/integration-test",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="clickhouse/integration-tests-runner",
        path="./docker/test/integration/runner",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/integration-test-with-unity-catalog",
        path="docker/test/integration/clickhouse_with_unity_catalog",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/integration-helper",
        path="./docker/test/integration/helper_container",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/kerberos-kdc",
        path="./docker/test/integration/kerberos_kdc",
        platforms=[Docker.Platforms.AMD],
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-golang-client",
        path="./docker/test/integration/mysql_golang_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-java-client",
        path="./docker/test/integration/mysql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-js-client",
        path="./docker/test/integration/mysql_js_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/dotnet-client",
        path="./docker/test/integration/dotnet_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-php-client",
        path="./docker/test/integration/mysql_php_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/nginx-dav",
        path="./docker/test/integration/nginx_dav",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/postgresql-java-client",
        path="./docker/test/integration/postgresql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/python-bottle",
        path="./docker/test/integration/resolver",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/s3-proxy",
        path="./docker/test/integration/s3_proxy",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/docs-builder",
        path="./ci/docker/docs-builder",
        platforms=Docker.Platforms.arm_amd,
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


class BuildTypes(metaclass=MetaClasses.WithIter):
    AMD_DEBUG = "amd_debug"
    AMD_RELEASE = "amd_release"
    AMD_BINARY = "amd_binary"
    AMD_ASAN = "amd_asan"
    AMD_TSAN = "amd_tsan"
    AMD_MSAN = "amd_msan"
    AMD_UBSAN = "amd_ubsan"
    ARM_RELEASE = "arm_release"
    ARM_ASAN = "arm_asan"

    ARM_COVERAGE = "arm_coverage"
    ARM_BINARY = "arm_binary"
    AMD_TIDY = "amd_tidy"
    ARM_TIDY = "arm_tidy"
    AMD_DARWIN = "amd_darwin"
    ARM_DARWIN = "arm_darwin"
    ARM_V80COMPAT = "arm_v80compat"
    AMD_FREEBSD = "amd_freebsd"
    PPC64LE = "ppc64le"
    AMD_COMPAT = "amd_compat"
    AMD_MUSL = "amd_musl"
    RISCV64 = "riscv64"
    S390X = "s390x"
    LOONGARCH64 = "loongarch64"
    FUZZERS = "fuzzers"


class JobNames:
    DOCKER_BUILDS_ARM = "Dockers build (arm)"
    DOCKER_BUILDS_AMD = "Dockers build (amd)"
    STYLE_CHECK = "Style check"
    FAST_TEST = "Fast test"
    BUILD = "Build"
    UNITTEST = "Unit tests"
    STATELESS = "Stateless tests"
    BUGFIX_VALIDATION = "Bugfix validation"
    STATEFUL = "Stateful tests"
    INTEGRATION = "Integration tests"
    STRESS = "Stress test"
    UPGRADE = "Upgrade check"
    PERFORMANCE = "Performance Comparison"
    COMPATIBILITY = "Compatibility check"
    Docs = "Docs check"
    CLICKBENCH = "ClickBench"
    DOCKER_SERVER = "Docker server image"
    DOCKER_KEEPER = "Docker keeper image"
    SQL_TEST = "SQLTest"
    SQLANCER = "SQLancer"
    INSTALL_TEST = "Install packages"
    ASTFUZZER = "AST fuzzer"
    BUZZHOUSE = "BuzzHouse"
    BUILDOCKER = "BuildDockers"
    BUGFIX_VALIDATE = "Bugfix validation"
    JEPSEN_KEEPER = "ClickHouse Keeper Jepsen"
    JEPSEN_SERVER = "ClickHouse Server Jepsen"
    LIBFUZZER_TEST = "libFuzzer tests"


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

    CH_COV_BIN = "CH_COV_BIN"
    CH_ARM_BIN = "CH_ARM_BIN"
    CH_TIDY_BIN = "CH_TIDY_BIN"
    CH_AMD_DARWIN_BIN = "CH_AMD_DARWIN_BIN"
    CH_ARM_DARWIN_BIN = "CH_ARM_DARWIN_BIN"
    CH_ARM_V80COMPAT = "CH_ARMV80C_DARWIN_BIN"
    CH_AMD_FREEBSD = "CH_ARM_FREEBSD_BIN"
    CH_PPC64LE = "CH_PPC64LE_BIN"
    CH_AMD_COMPAT = "CH_AMD_COMPAT_BIN"
    CH_AMD_MUSL = "CH_AMD_MUSL_BIN"
    CH_RISCV64 = "CH_RISCV64_BIN"
    CH_S390X = "CH_S390X_BIN"
    CH_LOONGARCH64 = "CH_LOONGARCH64_BIN"

    FAST_TEST = "FAST_TEST"
    UNITTEST_AMD_ASAN = "UNITTEST_AMD_ASAN"
    UNITTEST_AMD_TSAN = "UNITTEST_AMD_TSAN"
    UNITTEST_AMD_MSAN = "UNITTEST_AMD_MSAN"
    UNITTEST_AMD_UBSAN = "UNITTEST_AMD_UBSAN"

    DEB_AMD_DEBUG = "DEB_AMD_DEBUG"
    DEB_AMD_RELEASE = "DEB_AMD_RELEASE"
    DEB_COV = "DEB_COV"
    DEB_AMD_ASAN = "DEB_AMD_ASAN"
    DEB_AMD_TSAN = "DEB_AMD_TSAN"
    DEB_AMD_MSAM = "DEB_AMD_MSAM"
    DEB_AMD_UBSAN = "DEB_AMD_UBSAN"
    DEB_ARM_RELEASE = "DEB_ARM_RELEASE"
    DEB_ARM_ASAN = "DEB_ARM_ASAN"

    RPM_AMD_RELEASE = "RPM_AMD_RELEASE"
    RPM_ARM_RELEASE = "RPM_ARM_RELEASE"

    TGZ_AMD_RELEASE = "TGZ_AMD_RELEASE"
    TGZ_ARM_RELEASE = "TGZ_ARM_RELEASE"

    FUZZERS = "FUZZERS"
    FUZZERS_CORPUS = "FUZZERS_CORPUS"

    PERF_REPORTS_AMD_1 = "PERF_REPORTS_AMD_1"
    PERF_REPORTS_AMD_2 = "PERF_REPORTS_AMD_2"
    PERF_REPORTS_AMD_3 = "PERF_REPORTS_AMD_3"
    PERF_REPORTS_ARM_1 = "PERF_REPORTS_ARM_1"
    PERF_REPORTS_ARM_2 = "PERF_REPORTS_ARM_2"
    PERF_REPORTS_ARM_3 = "PERF_REPORTS_ARM_3"
    PERF_REPORTS_AMD_1_WITH_RELEASE = "PERF_REPORTS_AMD_1_WITH_RELEASE"
    PERF_REPORTS_AMD_2_WITH_RELEASE = "PERF_REPORTS_AMD_2_WITH_RELEASE"
    PERF_REPORTS_AMD_3_WITH_RELEASE = "PERF_REPORTS_AMD_3_WITH_RELEASE"

    PERF_REPORTS_ARM = "PERF_REPORTS_ARM"


class ArtifactConfigs:
    clickhouse_binaries = Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/self-extracting/clickhouse",
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
            ArtifactNames.CH_COV_BIN,
            ArtifactNames.CH_ARM_BIN,
            ArtifactNames.CH_TIDY_BIN,
            ArtifactNames.CH_AMD_DARWIN_BIN,
            ArtifactNames.CH_ARM_DARWIN_BIN,
            ArtifactNames.CH_ARM_V80COMPAT,
            ArtifactNames.CH_AMD_FREEBSD,
            ArtifactNames.CH_PPC64LE,
            ArtifactNames.CH_AMD_COMPAT,
            ArtifactNames.CH_AMD_MUSL,
            ArtifactNames.CH_RISCV64,
            ArtifactNames.CH_S390X,
            ArtifactNames.CH_LOONGARCH64,
        ]
    )
    clickhouse_debians = Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.deb",
    ).parametrize(
        names=[
            ArtifactNames.DEB_AMD_RELEASE,
            ArtifactNames.DEB_AMD_DEBUG,
            ArtifactNames.DEB_AMD_ASAN,
            ArtifactNames.DEB_AMD_TSAN,
            ArtifactNames.DEB_AMD_MSAM,
            ArtifactNames.DEB_AMD_UBSAN,
            ArtifactNames.DEB_COV,
            ArtifactNames.DEB_ARM_RELEASE,
            ArtifactNames.DEB_ARM_ASAN,
        ]
    )
    clickhouse_rpms = Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*.rpm",
    ).parametrize(
        names=[
            ArtifactNames.RPM_AMD_RELEASE,
            ArtifactNames.RPM_ARM_RELEASE,
        ]
    )
    clickhouse_tgzs = Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/*64.tgz*",
    ).parametrize(
        names=[
            ArtifactNames.TGZ_AMD_RELEASE,
            ArtifactNames.TGZ_ARM_RELEASE,
        ]
    )
    unittests_binaries = Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/src/unit_tests_dbms",
        compress_zst=True,
    ).parametrize(
        names=[
            ArtifactNames.UNITTEST_AMD_ASAN,
            ArtifactNames.UNITTEST_AMD_TSAN,
            ArtifactNames.UNITTEST_AMD_MSAN,
            ArtifactNames.UNITTEST_AMD_UBSAN,
        ]
    )
    fuzzers = Artifact.Config(
        name=ArtifactNames.FUZZERS,
        type=Artifact.Type.S3,
        path=[
            f"{TEMP_DIR}/build/programs/*_fuzzer",
            f"{TEMP_DIR}/build/programs/*_fuzzer.options",
            f"{TEMP_DIR}/build/programs/all.dict",
        ],
    )
    fuzzers_corpus = Artifact.Config(
        name=ArtifactNames.FUZZERS_CORPUS,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/*_seed_corpus.zip",
    )
    performance_reports = Artifact.Config(
        name="*",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/perf_wd/*.html",
    ).parametrize(
        names=[
            ArtifactNames.PERF_REPORTS_AMD_1,
            ArtifactNames.PERF_REPORTS_AMD_2,
            ArtifactNames.PERF_REPORTS_AMD_3,
            ArtifactNames.PERF_REPORTS_ARM_1,
            ArtifactNames.PERF_REPORTS_ARM_2,
            ArtifactNames.PERF_REPORTS_ARM_3,
            ArtifactNames.PERF_REPORTS_AMD_1_WITH_RELEASE,
            ArtifactNames.PERF_REPORTS_AMD_2_WITH_RELEASE,
            ArtifactNames.PERF_REPORTS_AMD_3_WITH_RELEASE,
        ]
    )


class Jobs:
    stateless_tests_jobs = Job.Config(
        name=JobNames.STATELESS,
        runs_on=["..params.."],
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
            # "amd_release,parallel",
            # "amd_release,non-parallel",
            # "arm_asan,parallel",
            # "arm_asan,non-parallel",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            # RunnerLabels.FUNC_TESTER_AMD,
            # RunnerLabels.FUNC_TESTER_AMD,
            # RunnerLabels.FUNC_TESTER_ARM,
            # RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
            [ArtifactNames.CH_AMD_DEBUG],
            # [ArtifactNames.CH_AMD_RELEASE],
            # [ArtifactNames.CH_AMD_RELEASE],
            # [ArtifactNames.CH_ARM_ASAN],
            # [ArtifactNames.CH_ARM_ASAN],
        ],
    )

    stateful_tests_jobs = Job.Config(
        name=JobNames.STATEFUL,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
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
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
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
    integration_test_jobs = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="python3 ./tests/ci/integration_test_check.py {PARAMETER}",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
            ],
        ),
        timeout=3600 * 2,
    ).parametrize(
        parameter=[f"{BuildTypes.AMD_ASAN},{i+1}/5" for i in range(5)],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(5)],
        requires=[[ArtifactNames.DEB_AMD_ASAN] for _ in range(5)],
    )

    # TODO: refactor job to be aligned with praktika style (remove wrappers, run in docker)
    stress_test_jobs = Job.Config(
        name=JobNames.STRESS,
        runs_on=RunnerLabels.BUILDER_ARM,
        command="python3 ./tests/ci/stress_check.py {PARAMETER}",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/stress_check.py", "./tests/docker_scripts/"]
        ),
    ).parametrize(
        parameter=[
            BuildTypes.ARM_RELEASE,
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_ARM,
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
            include_paths=[
                "./tests/ci/upgrade_check.py",
                "./tests/ci/stress_check.py",
                "./tests/docker_scripts/",
            ]
        ),
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.ARM_ASAN,
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            [ArtifactNames.DEB_AMD_DEBUG],
            [ArtifactNames.DEB_AMD_TSAN],
            [ArtifactNames.DEB_AMD_MSAM],
            [ArtifactNames.DEB_ARM_ASAN],
        ],
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
            RunnerLabels.FUNC_TESTER_AMD
            for _ in range(2)
            # RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[[ArtifactNames.CH_AMD_RELEASE] for _ in range(2)],
        # [ArtifactNames.CH_ARM_RELEASE]],
        provides=[
            [ArtifactNames.PERF_REPORTS_AMD_1_WITH_RELEASE],
            [ArtifactNames.PERF_REPORTS_AMD_2_WITH_RELEASE],
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
            RunnerLabels.STYLE_CHECK_AMD,
            RunnerLabels.STYLE_CHECK_ARM,
        ],
        requires=[[ArtifactNames.DEB_AMD_RELEASE], [ArtifactNames.DEB_ARM_RELEASE]],
    )
    docs_job = Job.Config(
        name=JobNames.Docs,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./tests/ci/docs_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=["**/*.md", "./docs", "tests/ci/docs_check.py"],
        ),
    )
    clickbench_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
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
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            [ArtifactNames.CH_AMD_RELEASE],
            [ArtifactNames.CH_ARM_RELEASE],
        ],
    )

    docker_job = Job.Config(
        name=JobNames.DOCKER_SERVER,
        # on ARM clickhouse-local call in docker build for amd leads to an error: Instruction check fail. The CPU does not support SSSE3 instruction set
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/docker_server_job.py --from-deb",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server_job.py",
                "./ci/docker/clickhouse-server",
            ],
        ),
        requires=[ArtifactNames.DEB_AMD_RELEASE, ArtifactNames.DEB_ARM_RELEASE],
    )

    # TODO: add tgz and rpm
    install_check_job = Job.Config(
        name=JobNames.INSTALL_TEST,
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
            RunnerLabels.STYLE_CHECK_AMD,
            RunnerLabels.STYLE_CHECK_ARM,
        ],
        requires=[
            [ArtifactNames.DEB_AMD_RELEASE, ArtifactNames.CH_AMD_RELEASE],
            [ArtifactNames.DEB_ARM_RELEASE, ArtifactNames.CH_ARM_RELEASE],
        ],
    )
    # ast_fuzzer_jobs = Job.Config(
    #     name=JobNames.ASTFUZZER,
    #     runs_on=RunnerLabels.FUNC_TESTER_ARM,
    #     command="python3 ./ci/jobs/fuzzers_job.py",
    #     requires=[ArtifactNames.CH_ARM_RELEASE],
    #     run_in_docker="clickhouse/stateless-test",
    # )
    # TODO: rewrite to praktika style job (commented above)
    ast_fuzzer_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=["..params.."],
        command=f"python3 ./tests/ci/ci_fuzzer_check.py {JobNames.ASTFUZZER}",
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_ASAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_UBSAN,
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(5)],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
            [ArtifactNames.CH_AMD_ASAN],
            [ArtifactNames.CH_AMD_TSAN],
            [ArtifactNames.CH_AMD_MSAN],
            [ArtifactNames.CH_AMD_UBSAN],
        ],
    )
    buzz_fuzzer_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=["..params.."],
        command=f"python3 ./tests/ci/ci_fuzzer_check.py {JobNames.BUZZHOUSE}",
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_ASAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_UBSAN,
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(5)],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
            [ArtifactNames.CH_AMD_ASAN],
            [ArtifactNames.CH_AMD_TSAN],
            [ArtifactNames.CH_AMD_MSAN],
            [ArtifactNames.CH_AMD_UBSAN],
        ],
    )
    docker_build_jobs = Job.Config(
        name=JobNames.BUILDOCKER,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./tests/ci/docker_tests_images.py --arch {PARAMETER}",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/docker_tests_images.py", "./docker"],
        ),
    ).parametrize(
        parameter=["arm", "amd", "multi"],
        runs_on=[
            RunnerLabels.STYLE_CHECK_ARM,
            RunnerLabels.STYLE_CHECK_AMD,
            RunnerLabels.STYLE_CHECK_ARM,
        ],
        requires=[
            [],
            [],
            [JobNames.BUILDOCKER + " (amd)", JobNames.BUILDOCKER + " (arm)"],
        ],
    )
