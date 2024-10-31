import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, List, Literal, Optional, Union

from ci_utils import WithIter
from integration_test_images import IMAGES


class Labels:
    PR_BUGFIX = "pr-bugfix"
    PR_CRITICAL_BUGFIX = "pr-critical-bugfix"
    CAN_BE_TESTED = "can be tested"
    DO_NOT_TEST = "do not test"
    MUST_BACKPORT = "pr-must-backport"
    MUST_BACKPORT_CLOUD = "pr-must-backport-cloud"
    JEPSEN_TEST = "jepsen-test"
    SKIP_MERGEABLE_CHECK = "skip mergeable check"
    PR_BACKPORT = "pr-backport"
    PR_BACKPORTS_CREATED = "pr-backports-created"
    PR_BACKPORTS_CREATED_CLOUD = "pr-backports-created-cloud"
    PR_CHERRYPICK = "pr-cherrypick"
    PR_CI = "pr-ci"
    PR_FEATURE = "pr-feature"
    PR_PERFORMANCE = "pr-performance"
    PR_SYNCED_TO_CLOUD = "pr-synced-to-cloud"
    PR_SYNC_UPSTREAM = "pr-sync-upstream"
    RELEASE = "release"
    RELEASE_LTS = "release-lts"
    SUBMODULE_CHANGED = "submodule changed"

    # automatic backport for critical bug fixes
    AUTO_BACKPORT = {"pr-critical-bugfix"}


class WorkflowStages(metaclass=WithIter):
    """
    Stages of GitHUb actions workflow
    """

    # for jobs that do not belong to any stage, e.g. Build Report Check
    NA = "UNKNOWN"
    # normal builds (builds that required for further testing)
    BUILDS_1 = "Builds_1"
    # special builds
    BUILDS_2 = "Builds_2"
    # all tests required for merge
    TESTS_1 = "Tests_1"
    # used in woolenwolfdog mode
    TESTS_2_WW = "Tests_2_ww"
    # all tests not required for merge
    TESTS_2 = "Tests_2"


class Runners(metaclass=WithIter):
    """
    GitHub runner's labels
    """

    BUILDER = "builder"
    BUILDER_ARM = "builder-aarch64"
    STYLE_CHECKER = "style-checker"
    STYLE_CHECKER_ARM = "style-checker-aarch64"
    FUNC_TESTER = "func-tester"
    FUNC_TESTER_ARM = "func-tester-aarch64"
    FUZZER_UNIT_TESTER = "fuzzer-unit-tester"


class Tags(metaclass=WithIter):
    """
    CI Customization tags (set via PR body or some of them in GH labels, e.g. libFuzzer)
    """

    DO_NOT_TEST_LABEL = "do_not_test"
    WOOLEN_WOLFDOG_LABEL = "woolen_wolfdog"
    NO_MERGE_COMMIT = "no_merge_commit"
    NO_CI_CACHE = "no_ci_cache"
    # to upload all binaries from build jobs
    UPLOAD_ALL_ARTIFACTS = "upload_all"
    CI_SET_SYNC = "ci_set_sync"
    CI_SET_ARM = "ci_set_arm"
    CI_SET_REQUIRED = "ci_set_required"
    CI_SET_BUILDS = "ci_set_builds"

    libFuzzer = "libFuzzer"


class WorkFlowNames(metaclass=WithIter):
    """
    CI WorkFlow Names for custom CI runs
    """

    JEPSEN = "JepsenWorkflow"
    CreateRelease = "CreateRelease"


class BuildNames(metaclass=WithIter):
    """
    Build' job names
    """

    PACKAGE_RELEASE = "package_release"
    PACKAGE_ASAN = "package_asan"
    PACKAGE_UBSAN = "package_ubsan"
    PACKAGE_TSAN = "package_tsan"
    PACKAGE_MSAN = "package_msan"
    PACKAGE_DEBUG = "package_debug"
    PACKAGE_AARCH64 = "package_aarch64"
    PACKAGE_ARM_ASAN = "package_aarch64_asan"
    PACKAGE_RELEASE_COVERAGE = "package_release_coverage"
    BINARY_RELEASE = "binary_release"
    BINARY_TIDY = "binary_tidy"
    BINARY_DARWIN = "binary_darwin"
    BINARY_AARCH64 = "binary_aarch64"
    BINARY_AARCH64_V80COMPAT = "binary_aarch64_v80compat"
    BINARY_FREEBSD = "binary_freebsd"
    BINARY_DARWIN_AARCH64 = "binary_darwin_aarch64"
    BINARY_PPC64LE = "binary_ppc64le"
    BINARY_AMD64_COMPAT = "binary_amd64_compat"
    BINARY_AMD64_MUSL = "binary_amd64_musl"
    BINARY_RISCV64 = "binary_riscv64"
    BINARY_S390X = "binary_s390x"
    BINARY_LOONGARCH64 = "binary_loongarch64"
    FUZZERS = "fuzzers"


class JobNames(metaclass=WithIter):
    """
    All CI non-build jobs (Build jobs are concatenated to this list via python hack)
    """

    STYLE_CHECK = "Style check"
    FAST_TEST = "Fast test"
    DOCKER_SERVER = "Docker server image"
    DOCKER_KEEPER = "Docker keeper image"
    INSTALL_TEST_AMD = "Install packages (release)"
    INSTALL_TEST_ARM = "Install packages (aarch64)"

    STATELESS_TEST_DEBUG = "Stateless tests (debug)"
    STATELESS_TEST_RELEASE = "Stateless tests (release)"
    STATELESS_TEST_RELEASE_COVERAGE = "Stateless tests (coverage)"
    STATELESS_TEST_AARCH64 = "Stateless tests (aarch64)"
    STATELESS_TEST_ASAN = "Stateless tests (asan)"
    STATELESS_TEST_ARM_ASAN = "Stateless tests (aarch64, asan)"
    STATELESS_TEST_TSAN = "Stateless tests (tsan)"
    STATELESS_TEST_MSAN = "Stateless tests (msan)"
    STATELESS_TEST_UBSAN = "Stateless tests (ubsan)"
    STATELESS_TEST_OLD_ANALYZER_S3_REPLICATED_RELEASE = (
        "Stateless tests (release, old analyzer, s3, DatabaseReplicated)"
    )
    STATELESS_TEST_S3_DEBUG = "Stateless tests (debug, s3 storage)"
    STATELESS_TEST_S3_TSAN = "Stateless tests (tsan, s3 storage)"
    STATELESS_TEST_AZURE_ASAN = "Stateless tests (azure, asan)"
    STATELESS_TEST_FLAKY_ASAN = "Stateless tests flaky check (asan)"

    STATEFUL_TEST_DEBUG = "Stateful tests (debug)"
    STATEFUL_TEST_RELEASE = "Stateful tests (release)"
    STATEFUL_TEST_RELEASE_COVERAGE = "Stateful tests (coverage)"
    STATEFUL_TEST_AARCH64 = "Stateful tests (aarch64)"
    STATEFUL_TEST_ASAN = "Stateful tests (asan)"
    STATEFUL_TEST_ARM_ASAN = "Stateful tests (aarch64, asan)"
    STATEFUL_TEST_TSAN = "Stateful tests (tsan)"
    STATEFUL_TEST_MSAN = "Stateful tests (msan)"
    STATEFUL_TEST_UBSAN = "Stateful tests (ubsan)"
    STATEFUL_TEST_PARALLEL_REPL_RELEASE = "Stateful tests (release, ParallelReplicas)"
    STATEFUL_TEST_PARALLEL_REPL_DEBUG = "Stateful tests (debug, ParallelReplicas)"
    STATEFUL_TEST_PARALLEL_REPL_ASAN = "Stateful tests (asan, ParallelReplicas)"
    STATEFUL_TEST_PARALLEL_REPL_MSAN = "Stateful tests (msan, ParallelReplicas)"
    STATEFUL_TEST_PARALLEL_REPL_UBSAN = "Stateful tests (ubsan, ParallelReplicas)"
    STATEFUL_TEST_PARALLEL_REPL_TSAN = "Stateful tests (tsan, ParallelReplicas)"

    STRESS_TEST_ASAN = "Stress test (asan)"
    STRESS_TEST_TSAN = "Stress test (tsan)"
    STRESS_TEST_UBSAN = "Stress test (ubsan)"
    STRESS_TEST_MSAN = "Stress test (msan)"
    STRESS_TEST_DEBUG = "Stress test (debug)"
    STRESS_TEST_AZURE_TSAN = "Stress test (azure, tsan)"
    STRESS_TEST_AZURE_MSAN = "Stress test (azure, msan)"

    INTEGRATION_TEST = "Integration tests (release)"
    INTEGRATION_TEST_ASAN = "Integration tests (asan)"
    INTEGRATION_TEST_ASAN_OLD_ANALYZER = "Integration tests (asan, old analyzer)"
    INTEGRATION_TEST_TSAN = "Integration tests (tsan)"
    INTEGRATION_TEST_ARM = "Integration tests (aarch64)"
    INTEGRATION_TEST_FLAKY = "Integration tests flaky check (asan)"

    UPGRADE_TEST_DEBUG = "Upgrade check (debug)"
    UPGRADE_TEST_ASAN = "Upgrade check (asan)"
    UPGRADE_TEST_TSAN = "Upgrade check (tsan)"
    UPGRADE_TEST_MSAN = "Upgrade check (msan)"

    UNIT_TEST = "Unit tests (release)"
    UNIT_TEST_ASAN = "Unit tests (asan)"
    UNIT_TEST_MSAN = "Unit tests (msan)"
    UNIT_TEST_TSAN = "Unit tests (tsan)"
    UNIT_TEST_UBSAN = "Unit tests (ubsan)"

    AST_FUZZER_TEST_DEBUG = "AST fuzzer (debug)"
    AST_FUZZER_TEST_ASAN = "AST fuzzer (asan)"
    AST_FUZZER_TEST_MSAN = "AST fuzzer (msan)"
    AST_FUZZER_TEST_TSAN = "AST fuzzer (tsan)"
    AST_FUZZER_TEST_UBSAN = "AST fuzzer (ubsan)"

    JEPSEN_KEEPER = "ClickHouse Keeper Jepsen"
    JEPSEN_SERVER = "ClickHouse Server Jepsen"

    PERFORMANCE_TEST_AMD64 = "Performance Comparison (release)"
    PERFORMANCE_TEST_ARM64 = "Performance Comparison (aarch64)"

    # SQL_LOGIC_TEST = "Sqllogic test (release)"

    SQLANCER = "SQLancer (release)"
    SQLANCER_DEBUG = "SQLancer (debug)"
    SQLTEST = "SQLTest"

    COMPATIBILITY_TEST = "Compatibility check (release)"
    COMPATIBILITY_TEST_ARM = "Compatibility check (aarch64)"

    CLICKBENCH_TEST = "ClickBench (release)"
    CLICKBENCH_TEST_ARM = "ClickBench (aarch64)"

    LIBFUZZER_TEST = "libFuzzer tests"

    BUILD_CHECK = "Builds"

    DOCS_CHECK = "Docs check"
    BUGFIX_VALIDATE = "Bugfix validation"


# hack to concatenate Build and non-build jobs under JobNames class
for attr_name in dir(BuildNames):
    if not attr_name.startswith("__") and not callable(getattr(BuildNames, attr_name)):
        setattr(JobNames, attr_name, getattr(BuildNames, attr_name))


class StatusNames(metaclass=WithIter):
    """
    Class with statuses that aren't related to particular jobs
    """

    # overall CI report
    CI = "CI running"
    # mergeable status
    MERGEABLE = "Mergeable Check"
    # status of a sync pr
    SYNC = "CH Inc sync"
    # PR formatting check status
    PR_CHECK = "PR Check"


class SyncState(metaclass=WithIter):
    PENDING = "awaiting sync"
    # temporary state if GH does not know mergeable state
    MERGE_UNKNOWN = "unknown state (might be auto recoverable)"
    # changes cannot be pushed/merged to a sync branch
    PUSH_FAILED = "push failed"
    MERGE_CONFLICTS = "merge conflicts"
    TESTING = "awaiting test results"
    TESTS_FAILED = "tests failed"
    COMPLETED = "completed"


@dataclass
class DigestConfig:
    # all files, dirs to include into digest, glob supported
    include_paths: List[Union[str, Path]] = field(default_factory=list)
    # file suffixes to exclude from digest
    exclude_files: List[str] = field(default_factory=list)
    # directories to exclude from digest
    exclude_dirs: List[Union[str, Path]] = field(default_factory=list)
    # docker names to include into digest
    docker: List[str] = field(default_factory=list)
    # git submodules digest
    git_submodules: bool = False


@dataclass
class LabelConfig:
    """
    configures different CI scenarios per CI Tag/GH label
    """

    run_jobs: Iterable[str] = frozenset()


@dataclass
class BuildConfig:
    name: str
    compiler: str
    package_type: Literal["deb", "binary", "fuzzers"]
    additional_pkgs: bool = False
    debug_build: bool = False
    coverage: bool = False
    sanitizer: str = ""
    tidy: bool = False
    # sparse_checkout is needed only to test the option itself.
    # No particular sense to use it in every build, since it slows down the job.
    sparse_checkout: bool = False
    comment: str = ""
    static_binary_name: str = ""

    def export_env(self, export: bool = False) -> str:
        def process(field_name: str, field: Union[bool, str]) -> str:
            if isinstance(field, bool):
                field = str(field).lower()
            elif not isinstance(field, str):
                field = ""
            if export:
                return f"export BUILD_{field_name.upper()}={repr(field)}"
            return f"BUILD_{field_name.upper()}={field}"

        return "\n".join(process(k, v) for k, v in self.__dict__.items())


@dataclass
class JobConfig:
    """
    contains config parameters for job execution in CI workflow
    """

    # GH Runner type (tag from @Runners)
    runner_type: str
    # used in ci unittests for config validation
    job_name_keyword: str = ""
    # name of another job that (if provided) should be used to check if job was affected by the change or not (in CiCache.has_evidence(job=@reference_job_name) call)
    # for example: "Stateless flaky check" can use reference_job_name="Stateless tests (release)". "Stateless flaky check" does not run on master
    #   and there cannot be an evidence for it, so instead "Stateless tests (release)" job name can be used to check the evidence
    reference_job_name: str = ""
    # builds required for the job (applicable for test jobs)
    required_builds: Optional[List[str]] = None
    # build config for the build job (applicable for builds)
    build_config: Optional[BuildConfig] = None
    # configures digest calculation for the job
    digest: DigestConfig = field(default_factory=DigestConfig)
    # will be triggered for the job if omitted in CI workflow yml
    run_command: str = ""
    # job timeout, seconds
    timeout: int = 7200
    # sets number of batches for a multi-batch job
    num_batches: int = 1
    # label that enables job in CI, if set digest isn't used
    run_by_labels: List[str] = field(default_factory=list)
    # to run always regardless of the job digest or/and label
    run_always: bool = False
    # disables CI await for a given job
    disable_await: bool = False
    # if the job needs to be run on the release branch, including master (building packages, docker server).
    # NOTE: Subsequent runs on the same branch with the similar digest are still considered skip-able.
    required_on_release_branch: bool = False
    # job is for pr workflow only
    pr_only: bool = False
    # job is for release/master branches only
    release_only: bool = False
    # to randomly pick and run one job among jobs in the same @random_bucket (PR branches only).
    random_bucket: str = ""
    # Do not set it. A list of batches to run. It will be set in runtime in accordance with ci cache and ci settings
    batches: Optional[List[int]] = None
    # Do not set it. A list of batches to await. It will be set in runtime in accordance with ci cache and ci settings
    pending_batches: Optional[List[int]] = None

    def with_properties(self, **kwargs: Any) -> "JobConfig":
        res = copy.deepcopy(self)
        for k, v in kwargs.items():
            assert hasattr(self, k), f"Setting invalid attribute [{k}]"
            setattr(res, k, v)
        return res

    def get_required_build(self) -> str:
        assert self.required_builds
        return self.required_builds[0]

    def has_digest(self) -> bool:
        return self.digest != DigestConfig()


class CommonJobConfigs:
    """
    Common job configs
    """

    BUILD_REPORT = JobConfig(
        job_name_keyword="builds",
        run_command="build_report_check.py",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/build_report_check.py",
                "./tests/ci/upload_result_helper.py",
            ],
        ),
        runner_type=Runners.STYLE_CHECKER_ARM,
        disable_await=True,
    )
    COMPATIBILITY_TEST = JobConfig(
        job_name_keyword="compatibility",
        digest=DigestConfig(
            include_paths=["./tests/ci/compatibility_check.py"],
            docker=["clickhouse/test-old-ubuntu", "clickhouse/test-old-centos"],
        ),
        run_command="compatibility_check.py",
        runner_type=Runners.STYLE_CHECKER,
    )
    INSTALL_TEST = JobConfig(
        job_name_keyword="install",
        digest=DigestConfig(
            include_paths=["./tests/ci/install_check.py"],
            docker=["clickhouse/install-deb-test", "clickhouse/install-rpm-test"],
        ),
        run_command='install_check.py "$CHECK_NAME"',
        runner_type=Runners.STYLE_CHECKER,
        timeout=900,
    )
    STATELESS_TEST = JobConfig(
        job_name_keyword="stateless",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
            ],
            exclude_files=[".md"],
            docker=["clickhouse/stateless-test"],
        ),
        run_command='functional_test_check.py "$CHECK_NAME"',
        runner_type=Runners.FUNC_TESTER,
    )
    STATEFUL_TEST = JobConfig(
        job_name_keyword="stateful",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/1_stateful/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
            ],
            exclude_files=[".md"],
            docker=["clickhouse/stateful-test"],
        ),
        run_command='functional_test_check.py "$CHECK_NAME"',
        runner_type=Runners.FUNC_TESTER,
        timeout=3600,
    )
    STRESS_TEST = JobConfig(
        job_name_keyword="stress",
        digest=DigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/queries/1_stateful/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
            ],
            exclude_files=[".md"],
            docker=["clickhouse/stress-test"],
        ),
        run_command="stress_check.py",
        runner_type=Runners.FUNC_TESTER,
        timeout=9000,
    )
    UPGRADE_TEST = JobConfig(
        job_name_keyword="upgrade",
        digest=DigestConfig(
            include_paths=["./tests/ci/upgrade_check.py", "./tests/docker_scripts/"],
            exclude_files=[".md"],
            docker=["clickhouse/stress-test"],
        ),
        run_command="upgrade_check.py",
        runner_type=Runners.FUNC_TESTER,
        timeout=3600,
    )
    INTEGRATION_TEST = JobConfig(
        job_name_keyword="integration",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
            ],
            exclude_files=[".md"],
            docker=IMAGES.copy(),
        ),
        run_command='integration_test_check.py "$CHECK_NAME"',
        runner_type=Runners.FUNC_TESTER,
    )
    ASTFUZZER_TEST = JobConfig(
        job_name_keyword="ast",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/ast_fuzzer_check.py",
            ],
            docker=["clickhouse/fuzzer"],
        ),
        run_command="ast_fuzzer_check.py",
        run_always=True,
        runner_type=Runners.FUZZER_UNIT_TESTER,
    )
    UNIT_TEST = JobConfig(
        job_name_keyword="unit",
        digest=DigestConfig(
            include_paths=["./tests/ci/unit_tests_check.py"],
            exclude_files=[".md"],
            docker=["clickhouse/unit-test"],
        ),
        run_command="unit_tests_check.py",
        runner_type=Runners.FUZZER_UNIT_TESTER,
    )
    PERF_TESTS = JobConfig(
        job_name_keyword="performance",
        digest=DigestConfig(
            include_paths=[
                "./tests/ci/performance_comparison_check.py",
                "./tests/performance/",
            ],
            exclude_files=[".md"],
            docker=["clickhouse/performance-comparison"],
        ),
        run_command="performance_comparison_check.py",
        runner_type=Runners.FUNC_TESTER,
    )
    SQLLANCER_TEST = JobConfig(
        job_name_keyword="lancer",
        digest=DigestConfig(),
        run_command="sqlancer_check.py",
        release_only=True,
        run_always=True,
        runner_type=Runners.FUZZER_UNIT_TESTER,
    )
    SQLLOGIC_TEST = JobConfig(
        job_name_keyword="logic",
        digest=DigestConfig(
            include_paths=["./tests/ci/sqllogic_test.py"],
            exclude_files=[".md"],
            docker=["clickhouse/sqllogic-test"],
        ),
        run_command="sqllogic_test.py",
        timeout=10800,
        release_only=True,
        runner_type=Runners.FUNC_TESTER,
    )
    SQL_TEST = JobConfig(
        job_name_keyword="sqltest",
        digest=DigestConfig(
            include_paths=["./tests/ci/sqltest.py"],
            exclude_files=[".md"],
            docker=["clickhouse/sqltest"],
        ),
        run_command="sqltest.py",
        timeout=10800,
        release_only=True,
        runner_type=Runners.FUZZER_UNIT_TESTER,
    )
    BUGFIX_TEST = JobConfig(
        job_name_keyword="bugfix",
        digest=DigestConfig(),
        run_command="bugfix_validate_check.py",
        timeout=900,
        runner_type=Runners.FUNC_TESTER,
    )
    DOCKER_SERVER = JobConfig(
        job_name_keyword="docker",
        required_on_release_branch=True,
        run_command='docker_server.py --check-name "$CHECK_NAME" --tag-type head --allow-build-reuse',
        digest=DigestConfig(
            include_paths=[
                "tests/ci/docker_server.py",
                "tests/ci/docker_images_helper.py",
                "./docker/server",
            ]
        ),
        runner_type=Runners.STYLE_CHECKER,
    )
    CLICKBENCH_TEST = JobConfig(
        job_name_keyword="clickbench",
        digest=DigestConfig(
            include_paths=[
                "tests/ci/clickbench.py",
            ],
            docker=["clickhouse/clickbench"],
        ),
        run_command='clickbench.py "$CHECK_NAME"',
        timeout=900,
        runner_type=Runners.FUNC_TESTER,
    )
    BUILD = JobConfig(
        required_on_release_branch=True,
        digest=DigestConfig(
            include_paths=[
                "./src",
                "./contrib/*-cmake",
                "./contrib/consistent-hashing",
                "./contrib/murmurhash",
                "./contrib/libfarmhash",
                "./contrib/pdqsort",
                "./contrib/cityhash102",
                "./contrib/sparse-checkout",
                "./contrib/libmetrohash",
                "./contrib/update-submodules.sh",
                "./contrib/CMakeLists.txt",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./packages",
                "./docker/packager/packager",
                "./rust",
                "./tests/ci/version_helper.py",
                # FIXME: This is a WA to rebuild the CH and recreate the Performance.tar.zst artifact
                # when there are changes in performance test scripts.
                # Due to the current design of the perf test we need to rebuild CH when the performance test changes,
                # otherwise the changes will not be visible in the PerformanceTest job in CI
                "./tests/performance",
            ],
            exclude_files=[".md"],
            docker=["clickhouse/binary-builder"],
            git_submodules=True,
        ),
        run_command="build_check.py $BUILD_NAME",
        runner_type=Runners.BUILDER,
    )


REQUIRED_CHECKS = [
    StatusNames.PR_CHECK,
    StatusNames.SYNC,
    JobNames.BUILD_CHECK,
    JobNames.DOCS_CHECK,
    JobNames.FAST_TEST,
    JobNames.STATEFUL_TEST_RELEASE,
    JobNames.STATELESS_TEST_RELEASE,
    JobNames.STATELESS_TEST_ASAN,
    JobNames.STATELESS_TEST_ARM_ASAN,
    JobNames.STATEFUL_TEST_ARM_ASAN,
    JobNames.STATELESS_TEST_FLAKY_ASAN,
    JobNames.STATEFUL_TEST_ASAN,
    JobNames.STYLE_CHECK,
    JobNames.UNIT_TEST_ASAN,
    JobNames.UNIT_TEST_MSAN,
    JobNames.UNIT_TEST,
    JobNames.UNIT_TEST_TSAN,
    JobNames.UNIT_TEST_UBSAN,
    JobNames.INTEGRATION_TEST_ASAN_OLD_ANALYZER,
    JobNames.STATELESS_TEST_OLD_ANALYZER_S3_REPLICATED_RELEASE,
]

# Jobs that run in Merge Queue if it's enabled
MQ_JOBS = [
    JobNames.STYLE_CHECK,
    JobNames.FAST_TEST,
    BuildNames.BINARY_RELEASE,
    JobNames.UNIT_TEST,
]
