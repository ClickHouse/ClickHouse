from praktika import Artifact, Docker, Secret
from praktika.utils import MetaClasses, Utils
from settings import altinity_overrides

# i.e. "ClickHouse/ci/tmp"
TEMP_DIR = f"{Utils.cwd()}/ci/tmp"  # == _Settings.TEMP_DIR != env_helper.TEMP_PATH

SYNC = "Altinity sync"

S3_BUCKET_NAME = altinity_overrides.S3_BUCKET_NAME
S3_REPORT_BUCKET_NAME = altinity_overrides.S3_REPORT_BUCKET_NAME
S3_BUCKET_HTTP_ENDPOINT = altinity_overrides.S3_BUCKET_HTTP_ENDPOINT
S3_REPORT_BUCKET_HTTP_ENDPOINT = altinity_overrides.S3_REPORT_BUCKET_HTTP_ENDPOINT


class RunnerLabels:
    CI_SERVICES = "ci_services"
    CI_SERVICES_EBS = "ci_services_ebs"
    BUILDER_AMD = ["self-hosted", "altinity-on-demand", "altinity-builder"]
    BUILDER_ARM = ["self-hosted", "altinity-on-demand", "altinity-builder"]
    FUNC_TESTER_AMD = ["self-hosted", "altinity-on-demand", "altinity-func-tester"]
    FUNC_TESTER_ARM = [
        "self-hosted",
        "altinity-on-demand",
        "altinity-func-tester-aarch64",
    ]
    AMD_LARGE = ["self-hosted", "altinity-on-demand", "altinity-func-tester"]
    ARM_LARGE = ["self-hosted", "altinity-on-demand", "altinity-func-tester-aarch64"]
    AMD_MEDIUM = ["self-hosted", "altinity-on-demand", "altinity-func-tester"]
    ARM_MEDIUM = ["self-hosted", "altinity-on-demand", "altinity-func-tester-aarch64"]
    AMD_MEDIUM_CPU = ["self-hosted", "altinity-on-demand", "altinity-func-tester"]
    ARM_MEDIUM_CPU = [
        "self-hosted",
        "altinity-on-demand",
        "altinity-func-tester-aarch64",
    ]
    AMD_MEDIUM_MEM = ["self-hosted", "altinity-on-demand", "altinity-func-tester"]
    ARM_MEDIUM_MEM = [
        "self-hosted",
        "altinity-on-demand",
        "altinity-func-tester-aarch64",
    ]
    AMD_SMALL = ["self-hosted", "altinity-on-demand", "altinity-style-checker"]
    ARM_SMALL = ["self-hosted", "altinity-on-demand", "altinity-style-checker-aarch64"]
    AMD_SMALL_MEM = ["self-hosted", "altinity-on-demand", "altinity-style-checker"]
    MACOS_ARM_SMALL = ["self-hosted", "arm_macos_small"]
    MACOS_AMD_SMALL = ["self-hosted", "amd_macos_m1"]
    STYLE_CHECK_AMD = ["self-hosted", "altinity-on-demand", "altinity-style-checker"]
    STYLE_CHECK_ARM = [
        "self-hosted",
        "altinity-on-demand",
        "altinity-style-checker-aarch64",
    ]


class CIFiles:
    UNIT_TESTS_RESULTS = f"{TEMP_DIR}/unit_tests_result.json"
    UNIT_TESTS_BIN = f"{TEMP_DIR}/build/src/unit_tests_dbms"


BASE_BRANCH = altinity_overrides.MAIN_BRANCH

azure_secret = Secret.Config(
    name="azure_connection_string",
    type=Secret.Type.AWS_SSM_PARAMETER,
)

chcache_secret = Secret.Config(
    name="chcache_password",
    type=Secret.Type.AWS_SSM_PARAMETER,
    region="us-east-1",
)

SECRETS = [
    Secret.Config(
        name=altinity_overrides.DOCKERHUB_SECRET,
        type=Secret.Type.GH_SECRET,
    ),
    Secret.Config(
        name=altinity_overrides.SECRET_CI_DB_URL,
        type=Secret.Type.GH_SECRET,
    ),
    Secret.Config(
        name=altinity_overrides.SECRET_CI_DB_USER,
        type=Secret.Type.GH_SECRET,
    ),
    Secret.Config(
        name=altinity_overrides.SECRET_CI_DB_PASSWORD,
        type=Secret.Type.GH_SECRET,
    ),
    # azure_secret,
    chcache_secret,
    # Secret.Config(
    #    name="woolenwolf_gh_app.clickhouse-app-id",
    #    type=Secret.Type.AWS_SSM_SECRET,
    # ),
    # Secret.Config(
    #    name="woolenwolf_gh_app.clickhouse-app-key",
    #    type=Secret.Type.AWS_SSM_SECRET,
    # ),
    Secret.Config(
        name="AWS_ACCESS_KEY_ID",
        type=Secret.Type.GH_SECRET,
    ),
    Secret.Config(
        name="AWS_SECRET_ACCESS_KEY",
        type=Secret.Type.GH_SECRET,
    ),
]

DOCKERS = [
    Docker.Config(
        name="altinityinfra/style-test",
        path="./ci/docker/style-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/fasttest",
        path="./ci/docker/fasttest",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/binary-builder",
        path="./ci/docker/binary-builder",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/fasttest"],
    ),
    Docker.Config(
        name="altinityinfra/stateless-test",
        path="./ci/docker/stateless-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/cctools",
        path="./ci/docker/cctools",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/fasttest"],
    ),
    Docker.Config(
        name="altinityinfra/test-base",
        path="./ci/docker/test-base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/stress-test",
        path="./ci/docker/stress-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/stateless-test"],
    ),
    Docker.Config(
        name="altinityinfra/fuzzer",
        path="./ci/docker/fuzzer",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/performance-comparison",
        path="./ci/docker/performance-comparison",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/keeper-jepsen-test",
        path="./ci/docker/keeper-jepsen-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/server-jepsen-test",
        path="./ci/docker/server-jepsen-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/integration-test",
        path="./ci/docker/integration/base",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/integration-tests-runner",
        path="./ci/docker/integration/runner",
        platforms=Docker.Platforms.arm_amd,
        depends_on=["altinityinfra/test-base"],
    ),
    Docker.Config(
        name="altinityinfra/integration-test-with-unity-catalog",
        path="./ci/docker/integration/clickhouse_with_unity_catalog",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/integration-test-with-hms",
        path="./ci/docker/integration/clickhouse_with_hms_catalog",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/integration-helper",
        path="./ci/docker/integration/helper_container",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/kerberos-kdc",
        path="./ci/docker/integration/kerberos_kdc",
        platforms=[Docker.Platforms.AMD],
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/test-mysql80",
        path="./ci/docker/integration/mysql80",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/test-mysql57",
        path="./ci/docker/integration/mysql57",
        platforms=Docker.Platforms.AMD,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/mysql-golang-client",
        path="./ci/docker/integration/mysql_golang_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/mysql-java-client",
        path="./ci/docker/integration/mysql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/mysql-js-client",
        path="./ci/docker/integration/mysql_js_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/arrowflight-server-test",
        path="./ci/docker/integration/arrowflight",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/dotnet-client",
        path="./ci/docker/integration/dotnet_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/mysql-php-client",
        path="./ci/docker/integration/mysql_php_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/nginx-dav",
        path="./ci/docker/integration/nginx_dav",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/postgresql-java-client",
        path="./ci/docker/integration/postgresql_java_client",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/python-bottle",
        path="./ci/docker/integration/resolver",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/s3-proxy",
        path="./ci/docker/integration/s3_proxy",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #    name="clickhouse/docs-builder",
    #     path="./ci/docker/docs-builder",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="altinityinfra/install-deb-test",
        path="./ci/docker/install/deb",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/install-rpm-test",
        path="./ci/docker/install/rpm",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/sqlancer-test",
        path="./ci/docker/sqlancer-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    Docker.Config(
        name="altinityinfra/mysql_dotnet_client",
        path="./ci/docker/integration/mysql_dotnet_client",
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
    ARM_TSAN = "arm_tsan"
    LLVM_COVERAGE_BUILD = "llvm_coverage_build"
    AMD_COVERAGE = "amd_coverage"
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
    ARM_FUZZERS = "arm_fuzzers"


class JobNames:
    DOCKER_BUILDS_ARM = "Dockers build (arm)"
    DOCKER_BUILDS_AMD = "Dockers build (amd)"
    STYLE_CHECK = "Style check"
    PR_BODY = "PR formatter"
    CODE_REVIEW = "Code Review"
    CI_RESULTS_REVIEW = "CI Results Review"
    FAST_TEST = "Fast test"
    SMOKE_TEST_MACOS = "Smoke test (amd_darwin)"
    BUILD = "Build"
    UNITTEST = "Unit tests"
    STATELESS = "Stateless tests"
    STATEFUL = "Stateful tests"
    INTEGRATION = "Integration tests"
    STRESS = "Stress test"
    UPGRADE = "Upgrade check"
    PERFORMANCE = "Performance Comparison"
    COMPATIBILITY = "Compatibility check"
    DOCS = "Docs check"
    DOCS_MINTLIFY = "Docs check (Mintlify)"
    CLICKBENCH = "ClickBench"
    DOCKER_SERVER = "Docker server image"
    DOCKER_KEEPER = "Docker keeper image"
    SQL_TEST = "SQLTest"
    SQL_LOGIC_TEST = "SQLLogic test"
    SQLANCER = "SQLancer"
    LLVM_COVERAGE = "LLVM Coverage"
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
    BUILD_TOOLCHAIN = "Build Toolchain (PGO, BOLT)"
    UPDATE_TOOLCHAIN_DOCKERFILE = "Update Toolchain Dockerfile"


class ToolSet:
    COMPILER_C = "clang-21"
    COMPILER_CPP = "clang++-21"

    COMPILER_CACHE = "sccache"
    COMPILER_CACHE_LEGACY = "sccache"


class ArtifactNames:
    CH_AMD_DEBUG = "CH_AMD_DEBUG"
    CH_AMD_LLVM_COVERAGE_BUILD = (
        "CH_AMD_LLVM_COVERAGE_BUILD"  # build with LLVM coverage enabled
    )
    LLVM_COVERAGE_FILE = "LLVM_COVERAGE_FILE"  # .profdata file
    LLVM_COVERAGE_INFO_FILE = "LLVM_COVERAGE_INFO_FILE"  # .info file generated from .profdata, used for debugging coverage results
    CH_AMD_RELEASE = "CH_AMD_RELEASE"
    CH_AMD_RELEASE_STRIPPED = "CH_AMD_RELEASE_STRIPPED"
    CH_AMD_ASAN = "CH_AMD_ASAN"
    CH_AMD_TSAN = "CH_AMD_TSAN"
    CH_AMD_MSAN = "CH_AMD_MSAN"
    CH_AMD_UBSAN = "CH_AMD_UBSAN"
    CH_AMD_BINARY = "CH_AMD_BINARY"
    CH_ARM_RELEASE = "CH_ARM_RELEASE"
    CH_ARM_RELEASE_STRIPPED = "CH_ARM_RELEASE_STRIPPED"
    CH_ARM_ASAN = "CH_ARM_ASAN"
    CH_ARM_TSAN = "CH_ARM_TSAN"

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
    UNITTEST_LLVM_COVERAGE = "UNITTEST_LLVM_COVERAGE"

    DEB_AMD_DEBUG = "DEB_AMD_DEBUG"
    DEB_AMD_RELEASE = "DEB_AMD_RELEASE"
    DEB_AMD_ASAN = "DEB_AMD_ASAN"
    DEB_AMD_TSAN = "DEB_AMD_TSAN"
    DEB_AMD_MSAN = "DEB_AMD_MSAM"
    DEB_AMD_UBSAN = "DEB_AMD_UBSAN"
    DEB_ARM_RELEASE = "DEB_ARM_RELEASE"
    DEB_ARM_ASAN = "DEB_ARM_ASAN"

    RPM_AMD_RELEASE = "RPM_AMD_RELEASE"
    RPM_ARM_RELEASE = "RPM_ARM_RELEASE"

    TGZ_AMD_RELEASE = "TGZ_AMD_RELEASE"
    TGZ_ARM_RELEASE = "TGZ_ARM_RELEASE"

    ARM_FUZZERS = "ARM_FUZZERS"
    FUZZERS_CORPUS = "FUZZERS_CORPUS"
    PARSER_MEMORY_PROFILER = "PARSER_MEMORY_PROFILER"

    TOOLCHAIN_PGO_BOLT_AMD = "TOOLCHAIN_PGO_BOLT_AMD"
    TOOLCHAIN_PGO_BOLT_ARM = "TOOLCHAIN_PGO_BOLT_ARM"


LLVM_FT_NUM_BATCHES = 3
LLVM_IT_NUM_BATCHES = 5
LLVM_FT_ARTIFACTS_LIST = [
    # default.profdata files for 3 batches from Stateless(Functional) tests
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_{batch}"
    for total_batches in (LLVM_FT_NUM_BATCHES,)
    for batch in range(1, total_batches + 1)
]

LLVM_FT_ARTIFACTS_LIST += [
    # default.profdata files for 6 jobs from Functional tests with Old Analyzer + S3 + AsyncInsert + parallel/sequential execution
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_parallel",
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_sequential",
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_parallel",
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_sequential",
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_parallel",
    ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_sequential",
]

LLVM_IT_ARTIFACTS_LIST = [
    # default.profdata files for 5 batches from Integration tests
    ArtifactNames.LLVM_COVERAGE_FILE + f"_it_{batch}"
    for total_batches in (LLVM_IT_NUM_BATCHES,)
    for batch in range(1, total_batches + 1)
]

LLVM_ARTIFACTS_LIST = (
    LLVM_FT_ARTIFACTS_LIST + LLVM_IT_ARTIFACTS_LIST + [ArtifactNames.LLVM_COVERAGE_FILE]
)

BINARIES_WITH_LONG_RETENTION = [
    ArtifactNames.CH_AMD_DEBUG,
    ArtifactNames.CH_AMD_RELEASE,
    ArtifactNames.CH_AMD_RELEASE_STRIPPED,
    ArtifactNames.CH_AMD_ASAN,
    ArtifactNames.CH_AMD_TSAN,
    ArtifactNames.CH_AMD_MSAN,
    ArtifactNames.CH_AMD_UBSAN,
    ArtifactNames.CH_AMD_BINARY,
    ArtifactNames.CH_ARM_RELEASE,
    ArtifactNames.CH_ARM_RELEASE_STRIPPED,
    ArtifactNames.CH_ARM_ASAN,
    ArtifactNames.CH_ARM_TSAN,
]


class ArtifactConfigs:
    clickhouse_binaries = Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/self-extracting/clickhouse",
    ).parametrize(
        names=[
            ArtifactNames.CH_AMD_DEBUG,
            ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
            ArtifactNames.CH_AMD_RELEASE,
            ArtifactNames.CH_AMD_ASAN,
            ArtifactNames.CH_AMD_TSAN,
            ArtifactNames.CH_AMD_MSAN,
            ArtifactNames.CH_AMD_UBSAN,
            ArtifactNames.CH_AMD_BINARY,
            ArtifactNames.CH_ARM_RELEASE,
            ArtifactNames.CH_ARM_ASAN,
            ArtifactNames.CH_ARM_TSAN,
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
    clickhouse_stripped_binaries = Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/programs/self-extracting/clickhouse-stripped",
    ).parametrize(
        names=[
            ArtifactNames.CH_AMD_RELEASE_STRIPPED,
            ArtifactNames.CH_ARM_RELEASE_STRIPPED,
        ]
    )
    llvm_profdata_file = Artifact.Config(
        name="...",
        type=Artifact.Type.S3,
        path=[
            f"./*.profdata",
        ],
    ).parametrize(names=LLVM_ARTIFACTS_LIST)

    llvm_coverage_info_file = Artifact.Config(
        name=ArtifactNames.LLVM_COVERAGE_INFO_FILE,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/llvm_coverage.info",
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
            ArtifactNames.DEB_AMD_MSAN,
            ArtifactNames.DEB_AMD_UBSAN,
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
            ArtifactNames.UNITTEST_LLVM_COVERAGE,
        ]
    )
    fuzzers = Artifact.Config(
        name=ArtifactNames.ARM_FUZZERS,
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
    parser_memory_profiler = Artifact.Config(
        name=ArtifactNames.PARSER_MEMORY_PROFILER,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/build/src/Parsers/examples/parser_memory_profiler",
    )
    toolchain_pgo_bolt_amd = Artifact.Config(
        name=ArtifactNames.TOOLCHAIN_PGO_BOLT_AMD,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/clang-pgo-bolt.tar.zst",
    )
    toolchain_pgo_bolt_arm = Artifact.Config(
        name=ArtifactNames.TOOLCHAIN_PGO_BOLT_ARM,
        type=Artifact.Type.S3,
        path=f"{TEMP_DIR}/clang-pgo-bolt.tar.zst",
    )
