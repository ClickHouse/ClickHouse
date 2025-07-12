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
    Docker.Config(
        name="clickhouse/stateless-test",
        path="./ci/docker/stateless-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/cctools",
        path="./ci/docker/cctools",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/fasttest"],
    ),
    Docker.Config(
        name="clickhouse/test-base",
        path="./ci/docker/test-base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/stress-test",
        path="./ci/docker/stress-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/stateless-test"],
    ),
    Docker.Config(
        name="clickhouse/fuzzer",
        path="./ci/docker/fuzzer",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/performance-comparison",
        path="./ci/docker/performance-comparison",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/keeper-jepsen-test",
        path="./ci/docker/keeper-jepsen-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/server-jepsen-test",
        path="./ci/docker/server-jepsen-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/integration-test",
        path="./ci/docker/integration/base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["clickhouse/test-base"],
    ),
    Docker.Config(
        name="clickhouse/integration-tests-runner",
        path="./ci/docker/integration/runner",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/integration-test-with-unity-catalog",
        path="./ci/docker/integration/clickhouse_with_unity_catalog",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/integration-helper",
        path="./ci/docker/integration/helper_container",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/kerberos-kdc",
        path="./ci/docker/integration/kerberos_kdc",
        platforms=[Docker.Platforms.AMD],
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-golang-client",
        path="./ci/docker/integration/mysql_golang_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-java-client",
        path="./ci/docker/integration/mysql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-js-client",
        path="./ci/docker/integration/mysql_js_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/dotnet-client",
        path="./ci/docker/integration/dotnet_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/mysql-php-client",
        path="./ci/docker/integration/mysql_php_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/nginx-dav",
        path="./ci/docker/integration/nginx_dav",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/postgresql-java-client",
        path="./ci/docker/integration/postgresql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/python-bottle",
        path="./ci/docker/integration/resolver",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/s3-proxy",
        path="./ci/docker/integration/s3_proxy",
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
        path="./ci/docker/install/deb",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="clickhouse/install-rpm-test",
        path="./ci/docker/install/rpm",
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
    BUGFIX_VALIDATE_IT = "Bugfix validation (integration tests)"
    BUGFIX_VALIDATE_FT = "Bugfix validation (functional tests)"
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
    CH_ARM_BINARY = "CH_ARM_BIN"
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
            ArtifactNames.CH_ARM_BINARY,
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
