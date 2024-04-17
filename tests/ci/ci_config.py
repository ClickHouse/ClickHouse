#!/usr/bin/env python3

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Literal, Optional, Union

from ci_utils import WithIter
from integration_test_images import IMAGES


class WorkFlows(metaclass=WithIter):
    PULL_REQUEST = "PULL_REQUEST"
    MASTER = "MASTER"
    BACKPORT = "BACKPORT"
    RELEASE = "RELEASE"
    SYNC = "SYNC"


class CIStages(metaclass=WithIter):
    NA = "UNKNOWN"
    BUILDS_1 = "Builds_1"
    BUILDS_2 = "Builds_2"
    TESTS_1 = "Tests_1"
    TESTS_2 = "Tests_2"


class Runners(metaclass=WithIter):
    BUILDER = "builder"
    STYLE_CHECKER = "style-checker"
    STYLE_CHECKER_ARM = "style-checker-aarch64"
    FUNC_TESTER = "func-tester"
    FUNC_TESTER_ARM = "func-tester-aarch64"
    STRESS_TESTER = "stress-tester"
    FUZZER_UNIT_TESTER = "fuzzer-unit-tester"


class Labels(metaclass=WithIter):
    """
    Label names or commit tokens in normalized form
    """

    DO_NOT_TEST_LABEL = "do_not_test"
    NO_MERGE_COMMIT = "no_merge_commit"
    NO_CI_CACHE = "no_ci_cache"
    CI_SET_REDUCED = "ci_set_reduced"
    CI_SET_FAST = "ci_set_fast"
    CI_SET_ARM = "ci_set_arm"
    CI_SET_INTEGRATION = "ci_set_integration"
    CI_SET_ANALYZER = "ci_set_analyzer"
    CI_SET_STATLESS = "ci_set_stateless"
    CI_SET_STATEFUL = "ci_set_stateful"
    CI_SET_STATLESS_ASAN = "ci_set_stateless_asan"
    CI_SET_STATEFUL_ASAN = "ci_set_stateful_asan"

    libFuzzer = "libFuzzer"


class Build(metaclass=WithIter):
    PACKAGE_RELEASE = "package_release"
    PACKAGE_AARCH64 = "package_aarch64"
    PACKAGE_ASAN = "package_asan"
    PACKAGE_UBSAN = "package_ubsan"
    PACKAGE_TSAN = "package_tsan"
    PACKAGE_MSAN = "package_msan"
    PACKAGE_DEBUG = "package_debug"
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
    # BINARY_S390X = "binary_s390x" # disabled because s390x refused to build in the migration to OpenSSL
    FUZZERS = "fuzzers"


class JobNames(metaclass=WithIter):
    STYLE_CHECK = "Style check"
    FAST_TEST = "Fast test"
    DOCKER_SERVER = "Docker server image"
    DOCKER_KEEPER = "Docker keeper image"
    INSTALL_TEST_AMD = "Install packages (amd64)"
    INSTALL_TEST_ARM = "Install packages (arm64)"

    STATELESS_TEST_DEBUG = "Stateless tests (debug)"
    STATELESS_TEST_RELEASE = "Stateless tests (release)"
    STATELESS_TEST_RELEASE_COVERAGE = "Stateless tests (coverage)"
    STATELESS_TEST_AARCH64 = "Stateless tests (aarch64)"
    STATELESS_TEST_ASAN = "Stateless tests (asan)"
    STATELESS_TEST_TSAN = "Stateless tests (tsan)"
    STATELESS_TEST_MSAN = "Stateless tests (msan)"
    STATELESS_TEST_UBSAN = "Stateless tests (ubsan)"
    STATELESS_TEST_ANALYZER_S3_REPLICATED_RELEASE = (
        "Stateless tests (release, old analyzer, s3, DatabaseReplicated)"
    )
    # merged into STATELESS_TEST_ANALYZER_S3_REPLICATED_RELEASE:
    # STATELESS_TEST_ANALYZER_RELEASE = "Stateless tests (release, analyzer)"
    # STATELESS_TEST_DB_REPL_RELEASE = "Stateless tests (release, DatabaseReplicated)"
    # STATELESS_TEST_S3_RELEASE = "Stateless tests (release, s3 storage)"
    STATELESS_TEST_S3_DEBUG = "Stateless tests (debug, s3 storage)"
    STATELESS_TEST_S3_TSAN = "Stateless tests (tsan, s3 storage)"
    STATELESS_TEST_FLAKY_ASAN = "Stateless tests flaky check (asan)"

    STATEFUL_TEST_DEBUG = "Stateful tests (debug)"
    STATEFUL_TEST_RELEASE = "Stateful tests (release)"
    STATEFUL_TEST_RELEASE_COVERAGE = "Stateful tests (coverage)"
    STATEFUL_TEST_AARCH64 = "Stateful tests (aarch64)"
    STATEFUL_TEST_ASAN = "Stateful tests (asan)"
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

    INTEGRATION_TEST = "Integration tests (release)"
    INTEGRATION_TEST_ASAN = "Integration tests (asan)"
    INTEGRATION_TEST_ASAN_ANALYZER = "Integration tests (asan, old analyzer)"
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

    PERFORMANCE_TEST_AMD64 = "Performance Comparison"
    PERFORMANCE_TEST_ARM64 = "Performance Comparison Aarch64"

    SQL_LOGIC_TEST = "Sqllogic test (release)"

    SQLANCER = "SQLancer (release)"
    SQLANCER_DEBUG = "SQLancer (debug)"
    SQLTEST = "SQLTest"

    COMPATIBILITY_TEST = "Compatibility check (amd64)"
    COMPATIBILITY_TEST_ARM = "Compatibility check (aarch64)"

    CLCIKBENCH_TEST = "ClickBench (amd64)"
    CLCIKBENCH_TEST_ARM = "ClickBench (aarch64)"

    LIBFUZZER_TEST = "libFuzzer tests"

    BUILD_CHECK = "ClickHouse build check"
    BUILD_CHECK_SPECIAL = "ClickHouse special build check"

    DOCS_CHECK = "Docs check"
    BUGFIX_VALIDATE = "Bugfix validation"


class StatusNames(metaclass=WithIter):
    "Class with statuses that aren't related to particular jobs"
    CI = "CI running"
    MERGEABLE = "Mergeable Check"
    SYNC = "A Sync"


# dynamically update JobName with Build jobs
for attr_name in dir(Build):
    if not attr_name.startswith("__") and not callable(getattr(Build, attr_name)):
        setattr(JobNames, attr_name, getattr(Build, attr_name))


@dataclass
class DigestConfig:
    # all files, dirs to include into digest, glob supported
    include_paths: List[Union[str, Path]] = field(default_factory=list)
    # file suffixes to exclude from digest
    exclude_files: List[str] = field(default_factory=list)
    # directories to exlude from digest
    exclude_dirs: List[Union[str, Path]] = field(default_factory=list)
    # docker names to include into digest
    docker: List[str] = field(default_factory=list)
    # git submodules digest
    git_submodules: bool = False


@dataclass
class LabelConfig:
    """
    configures different CI scenarious per GH label
    """

    run_jobs: Iterable[str] = frozenset()


@dataclass
class JobConfig:
    """
    contains config parameters for job execution in CI workflow
    """

    # configures digest calculation for the job
    digest: DigestConfig = field(default_factory=DigestConfig)
    # will be triggered for the job if omited in CI workflow yml
    run_command: str = ""
    # job timeout, seconds
    timeout: Optional[int] = None
    # sets number of batches for multi-batch job
    num_batches: int = 1
    # label that enables job in CI, if set digest won't be used
    run_by_label: str = ""
    # to run always regardless of the job digest or/and label
    run_always: bool = False
    # if the job needs to be run on the release branch, including master (e.g. building packages, docker server).
    # NOTE: Subsequent runs on the same branch with the similar digest are still considered skippable.
    required_on_release_branch: bool = False
    # job is for pr workflow only
    pr_only: bool = False
    # job is for release/master branches only
    release_only: bool = False
    # to randomly pick and run one job among jobs in the same @random_bucket. Applied in PR branches only.
    random_bucket: str = ""


builds_job_config = JobConfig(
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
)
fuzzer_build_job_config = deepcopy(builds_job_config)
fuzzer_build_job_config.run_by_label = Labels.libFuzzer


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
    job_config: JobConfig = field(default_factory=lambda: deepcopy(builds_job_config))

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
class BuildReportConfig:
    builds: List[str]
    job_config: JobConfig = field(
        default_factory=lambda: JobConfig(
            run_command='build_report_check.py "$CHECK_NAME"',
            digest=DigestConfig(
                include_paths=[
                    "./tests/ci/build_report_check.py",
                    "./tests/ci/upload_result_helper.py",
                ],
            ),
        )
    )


@dataclass
class TestConfig:
    required_build: str
    job_config: JobConfig = field(default_factory=JobConfig)


BuildConfigs = Dict[str, BuildConfig]
BuildsReportConfig = Dict[str, BuildReportConfig]
TestConfigs = Dict[str, TestConfig]
LabelConfigs = Dict[str, LabelConfig]

# common digests configs
compatibility_check_digest = DigestConfig(
    include_paths=["./tests/ci/compatibility_check.py"],
    docker=["clickhouse/test-old-ubuntu", "clickhouse/test-old-centos"],
)
install_check_digest = DigestConfig(
    include_paths=["./tests/ci/install_check.py"],
    docker=["clickhouse/install-deb-test", "clickhouse/install-rpm-test"],
)
stateless_check_digest = DigestConfig(
    include_paths=[
        "./tests/ci/functional_test_check.py",
        "./tests/queries/0_stateless/",
        "./tests/clickhouse-test",
        "./tests/config",
        "./tests/*.txt",
    ],
    exclude_files=[".md"],
    docker=["clickhouse/stateless-test"],
)
stateful_check_digest = DigestConfig(
    include_paths=[
        "./tests/ci/functional_test_check.py",
        "./tests/queries/1_stateful/",
        "./tests/clickhouse-test",
        "./tests/config",
        "./tests/*.txt",
    ],
    exclude_files=[".md"],
    docker=["clickhouse/stateful-test"],
)

stress_check_digest = DigestConfig(
    include_paths=[
        "./tests/queries/0_stateless/",
        "./tests/queries/1_stateful/",
        "./tests/clickhouse-test",
        "./tests/config",
        "./tests/*.txt",
    ],
    exclude_files=[".md"],
    docker=["clickhouse/stress-test"],
)
# FIXME: which tests are upgrade? just python?
upgrade_check_digest = DigestConfig(
    include_paths=["./tests/ci/upgrade_check.py"],
    exclude_files=[".md"],
    docker=["clickhouse/upgrade-check"],
)
integration_check_digest = DigestConfig(
    include_paths=[
        "./tests/ci/integration_test_check.py",
        "./tests/ci/integration_tests_runner.py",
        "./tests/integration/",
    ],
    exclude_files=[".md"],
    docker=IMAGES.copy(),
)

ast_fuzzer_check_digest = DigestConfig(
    # include_paths=["./tests/ci/ast_fuzzer_check.py"],
    # exclude_files=[".md"],
    # docker=["clickhouse/fuzzer"],
)
unit_check_digest = DigestConfig(
    include_paths=["./tests/ci/unit_tests_check.py"],
    exclude_files=[".md"],
    docker=["clickhouse/unit-test"],
)
perf_check_digest = DigestConfig(
    include_paths=[
        "./tests/ci/performance_comparison_check.py",
        "./tests/performance/",
    ],
    exclude_files=[".md"],
    docker=["clickhouse/performance-comparison"],
)
sqllancer_check_digest = DigestConfig(
    # include_paths=["./tests/ci/sqlancer_check.py"],
    # exclude_files=[".md"],
    # docker=["clickhouse/sqlancer-test"],
)
sqllogic_check_digest = DigestConfig(
    include_paths=["./tests/ci/sqllogic_test.py"],
    exclude_files=[".md"],
    docker=["clickhouse/sqllogic-test"],
)
sqltest_check_digest = DigestConfig(
    include_paths=["./tests/ci/sqltest.py"],
    exclude_files=[".md"],
    docker=["clickhouse/sqltest"],
)
bugfix_validate_check = DigestConfig(
    include_paths=[
        "./tests/queries/0_stateless/",
        "./tests/ci/integration_test_check.py",
        "./tests/ci/functional_test_check.py",
        "./tests/ci/bugfix_validate_check.py",
    ],
    exclude_files=[".md"],
    docker=IMAGES.copy()
    + [
        "clickhouse/stateless-test",
    ],
)
# common test params
docker_server_job_config = JobConfig(
    required_on_release_branch=True,
    run_command='docker_server.py --check-name "$CHECK_NAME" --release-type head --allow-build-reuse',
    digest=DigestConfig(
        include_paths=[
            "tests/ci/docker_server.py",
            "./docker/server",
        ]
    ),
)
compatibility_test_common_params = {
    "digest": compatibility_check_digest,
    "run_command": "compatibility_check.py",
}
statless_test_common_params = {
    "digest": stateless_check_digest,
    "run_command": 'functional_test_check.py "$CHECK_NAME" $KILL_TIMEOUT',
    "timeout": 10800,
}
stateful_test_common_params = {
    "digest": stateful_check_digest,
    "run_command": 'functional_test_check.py "$CHECK_NAME" $KILL_TIMEOUT',
    "timeout": 3600,
}
stress_test_common_params = {
    "digest": stress_check_digest,
    "run_command": "stress_check.py",
}
upgrade_test_common_params = {
    "digest": upgrade_check_digest,
    "run_command": "upgrade_check.py",
}
astfuzzer_test_common_params = {
    "digest": ast_fuzzer_check_digest,
    "run_command": "ast_fuzzer_check.py",
    "run_always": True,
}
integration_test_common_params = {
    "digest": integration_check_digest,
    "run_command": 'integration_test_check.py "$CHECK_NAME"',
}
unit_test_common_params = {
    "digest": unit_check_digest,
    "run_command": "unit_tests_check.py",
}
perf_test_common_params = {
    "digest": perf_check_digest,
    "run_command": "performance_comparison_check.py",
}
sqllancer_test_common_params = JobConfig(
    digest=sqllancer_check_digest,
    run_command="sqlancer_check.py",
    release_only=True,
    run_always=True,
)
sqllogic_test_params = JobConfig(
    digest=sqllogic_check_digest,
    run_command="sqllogic_test.py",
    timeout=10800,
    release_only=True,
)
sql_test_params = JobConfig(
    digest=sqltest_check_digest,
    run_command="sqltest.py",
    timeout=10800,
    release_only=True,
)
clickbench_test_params = {
    "digest": DigestConfig(
        include_paths=[
            "tests/ci/clickbench.py",
        ],
        docker=["clickhouse/clickbench"],
    ),
    "run_command": 'clickbench.py "$CHECK_NAME"',
}
install_test_params = JobConfig(
    digest=install_check_digest,
    run_command='install_check.py "$CHECK_NAME"',
    timeout=900,
)


@dataclass
class CIConfig:
    """
    Contains configs for all jobs in the CI pipeline
    each config item in the below dicts should be an instance of JobConfig class or inherited from it
    """

    build_config: BuildConfigs
    builds_report_config: BuildsReportConfig
    test_configs: TestConfigs
    other_jobs_configs: TestConfigs
    label_configs: LabelConfigs

    def get_label_config(self, label_name: str) -> Optional[LabelConfig]:
        for label, config in self.label_configs.items():
            if self.normalize_string(label_name) == self.normalize_string(label):
                return config
        return None

    def get_job_ci_stage(self, job_name: str) -> str:
        if job_name in [
            JobNames.STYLE_CHECK,
            JobNames.FAST_TEST,
            JobNames.JEPSEN_KEEPER,
            JobNames.BUILD_CHECK,
            JobNames.BUILD_CHECK_SPECIAL,
        ]:
            # FIXME: we can't currently handle Jepsen in the Stage as it's job has concurrency directive
            # BUILD_CHECK and BUILD_CHECK_SPECIAL runs not in stage because we need them even if Builds stage failed
            return CIStages.NA
        stage_type = None
        if self.is_build_job(job_name):
            stage_type = CIStages.BUILDS_1
            if job_name in CI_CONFIG.get_builds_for_report(
                JobNames.BUILD_CHECK_SPECIAL
            ):
                # special builds go to Build_2 stage to not delay Builds_1/Test_1
                stage_type = CIStages.BUILDS_2
        elif self.is_docs_job(job_name):
            stage_type = CIStages.TESTS_1
        elif job_name == JobNames.BUILD_CHECK_SPECIAL:
            stage_type = CIStages.TESTS_2
        elif self.is_test_job(job_name):
            stage_type = CIStages.TESTS_1
            if job_name in CI_CONFIG.test_configs:
                required_build = CI_CONFIG.test_configs[job_name].required_build
                assert required_build
                if required_build in CI_CONFIG.get_builds_for_report(
                    JobNames.BUILD_CHECK
                ):
                    stage_type = CIStages.TESTS_1
                else:
                    stage_type = CIStages.TESTS_2
            else:
                stage_type = CIStages.TESTS_1
        assert stage_type, f"BUG [{job_name}]"
        return stage_type

    def get_job_config(self, check_name: str) -> JobConfig:
        res = None
        for config in (
            self.build_config,
            self.builds_report_config,
            self.test_configs,
            self.other_jobs_configs,
        ):
            if check_name in config:  # type: ignore
                res = config[check_name].job_config  # type: ignore
                break
        return res  # type: ignore

    def get_runner_type(self, check_name: str) -> str:
        result = None
        if self.is_build_job(check_name) or check_name == JobNames.FAST_TEST:
            result = Runners.BUILDER
        elif any(
            words in check_name.lower()
            for words in [
                "install packages",
                "compatibility check",
                "docker",
                "build check",
                "jepsen",
                "style check",
            ]
        ):
            result = Runners.STYLE_CHECKER
        elif check_name == JobNames.DOCS_CHECK:
            # docs job is demanding
            result = Runners.FUNC_TESTER_ARM
        elif any(
            words in check_name.lower()
            for words in [
                "stateless",
                "stateful",
                "clickbench",
                "sqllogic test",
                "libfuzzer",
                "bugfix validation",
            ]
        ):
            result = Runners.FUNC_TESTER
        elif any(
            words in check_name.lower()
            for words in ["stress", "upgrade", "integration", "performance comparison"]
        ):
            result = Runners.STRESS_TESTER
        elif any(
            words in check_name.lower()
            for words in ["ast fuzzer", "unit tests", "sqlancer", "sqltest"]
        ):
            result = Runners.FUZZER_UNIT_TESTER

        assert result, f"BUG, no runner for [{check_name}]"

        if (
            "aarch" in check_name.lower() or "arm64" in check_name.lower()
        ) and "aarch" not in result:
            if result == Runners.STRESS_TESTER:
                # FIXME: no arm stress tester group atm
                result = Runners.FUNC_TESTER_ARM
            elif result == Runners.BUILDER:
                # crosscompile - no arm required
                pass
            else:
                # switch to aarch64 runnner
                result += "-aarch64"

        return result

    @staticmethod
    def normalize_string(input_string: str) -> str:
        lowercase_string = input_string.lower()
        normalized_string = (
            lowercase_string.replace(" ", "_")
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
        )
        return normalized_string

    def get_job_with_parents(self, check_name: str) -> List[str]:
        res = []
        check_name = self.normalize_string(check_name)

        for config in (
            self.build_config,
            self.builds_report_config,
            self.test_configs,
            self.other_jobs_configs,
        ):
            for job_name in config:  # type: ignore
                if check_name == self.normalize_string(job_name):
                    res.append(job_name)
                    if isinstance(config[job_name], TestConfig):  # type: ignore
                        if config[job_name].required_build:  # type: ignore
                            res.append(config[job_name].required_build)  # type: ignore
                    elif isinstance(config[job_name], BuildConfig):  # type: ignore
                        pass
                    elif isinstance(config[job_name], BuildReportConfig):  # type: ignore
                        # add all build jobs as parents for build report check
                        res.extend(
                            [job for job in JobNames if job in self.build_config]
                        )
                    else:
                        assert (
                            False
                        ), f"check commit message tags or FIXME: request for job [{check_name}] not yet supported"
                    break
        assert (
            res
        ), f"Error: Experimantal feature... Invlid request or not supported job [{check_name}]"
        return res

    def get_digest_config(self, check_name: str) -> DigestConfig:
        res = None
        for config in (
            self.other_jobs_configs,
            self.build_config,
            self.builds_report_config,
            self.test_configs,
        ):
            if check_name in config:  # type: ignore
                res = config[check_name].job_config.digest  # type: ignore
        assert (
            res
        ), f"Invalid check_name or CI_CONFIG outdated, config not found for [{check_name}]"
        return res  # type: ignore

    def job_generator(self, branch: str) -> Iterable[str]:
        """
        traverses all check names in CI pipeline
        """
        assert branch
        for config in (
            self.other_jobs_configs,
            self.build_config,
            self.builds_report_config,
            self.test_configs,
        ):
            yield from config  # type: ignore

    def get_builds_for_report(
        self, report_name: str, release: bool = False, backport: bool = False
    ) -> List[str]:
        # hack to modify build list for release and bp wf
        assert not (release and backport), "Invalid input"
        if backport and report_name == JobNames.BUILD_CHECK:
            return [
                Build.PACKAGE_RELEASE,
                Build.PACKAGE_AARCH64,
                Build.PACKAGE_ASAN,
                Build.PACKAGE_TSAN,
                Build.PACKAGE_DEBUG,
            ]
        if (release or backport) and report_name == JobNames.BUILD_CHECK_SPECIAL:
            return [
                Build.BINARY_DARWIN,
                Build.BINARY_DARWIN_AARCH64,
            ]

        return self.builds_report_config[report_name].builds

    @classmethod
    def is_build_job(cls, job: str) -> bool:
        return job in Build

    @classmethod
    def is_test_job(cls, job: str) -> bool:
        return not cls.is_build_job(job) and job != JobNames.STYLE_CHECK

    @classmethod
    def is_docs_job(cls, job: str) -> bool:
        return job == JobNames.DOCS_CHECK

    def validate(self) -> None:
        errors = []
        for name, build_config in self.build_config.items():
            build_in_reports = False
            for _, report_config in self.builds_report_config.items():
                if name in report_config.builds:
                    build_in_reports = True
                    break
            # All build configs must belong to build_report_config
            if not build_in_reports:
                logging.error("Build name %s does not belong to build reports", name)
                errors.append(f"Build name {name} does not belong to build reports")
            # The name should be the same as build_config.name
            if not build_config.name == name:
                logging.error(
                    "Build name '%s' does not match the config 'name' value '%s'",
                    name,
                    build_config.name,
                )
                errors.append(
                    f"Build name {name} does not match 'name' value '{build_config.name}'"
                )
        # All build_report_config values should be in build_config.keys()
        for build_report_name, build_report_config in self.builds_report_config.items():
            build_names = build_report_config.builds
            missed_names = [
                name for name in build_names if name not in self.build_config.keys()
            ]
            if missed_names:
                logging.error(
                    "The following names of the build report '%s' "
                    "are missed in build_config: %s",
                    build_report_name,
                    missed_names,
                )
                errors.append(
                    f"The following names of the build report '{build_report_name}' "
                    f"are missed in build_config: {missed_names}",
                )
        # And finally, all of tests' requirements must be in the builds
        for test_name, test_config in self.test_configs.items():
            if test_config.required_build not in self.build_config.keys():
                logging.error(
                    "The requierment '%s' for '%s' is not found in builds",
                    test_config,
                    test_name,
                )
                errors.append(
                    f"The requierment '{test_config}' for "
                    f"'{test_name}' is not found in builds"
                )

        if errors:
            raise KeyError("config contains errors", errors)


CI_CONFIG = CIConfig(
    label_configs={
        Labels.DO_NOT_TEST_LABEL: LabelConfig(run_jobs=[JobNames.STYLE_CHECK]),
        Labels.CI_SET_FAST: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
            ]
        ),
        Labels.CI_SET_ARM: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                Build.PACKAGE_AARCH64,
                JobNames.INTEGRATION_TEST_ARM,
            ]
        ),
        Labels.CI_SET_INTEGRATION: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                Build.PACKAGE_RELEASE,
                JobNames.INTEGRATION_TEST,
            ]
        ),
        Labels.CI_SET_ANALYZER: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                Build.PACKAGE_RELEASE,
                Build.PACKAGE_ASAN,
                JobNames.STATELESS_TEST_ANALYZER_S3_REPLICATED_RELEASE,
                JobNames.INTEGRATION_TEST_ASAN_ANALYZER,
            ]
        ),
        Labels.CI_SET_STATLESS: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                Build.PACKAGE_RELEASE,
                JobNames.STATELESS_TEST_RELEASE,
            ]
        ),
        Labels.CI_SET_STATLESS_ASAN: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                Build.PACKAGE_ASAN,
                JobNames.STATELESS_TEST_ASAN,
            ]
        ),
        Labels.CI_SET_STATEFUL: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                Build.PACKAGE_RELEASE,
                JobNames.STATEFUL_TEST_RELEASE,
            ]
        ),
        Labels.CI_SET_STATEFUL_ASAN: LabelConfig(
            run_jobs=[
                JobNames.STYLE_CHECK,
                JobNames.FAST_TEST,
                Build.PACKAGE_ASAN,
                JobNames.STATEFUL_TEST_ASAN,
            ]
        ),
        Labels.CI_SET_REDUCED: LabelConfig(
            run_jobs=[
                job
                for job in JobNames
                if not any(
                    nogo in job
                    for nogo in (
                        "asan",
                        "tsan",
                        "msan",
                        "ubsan",
                        "coverage",
                        # skip build report jobs as not all builds will be done
                        "build check",
                    )
                )
            ]
        ),
    },
    build_config={
        Build.PACKAGE_RELEASE: BuildConfig(
            name=Build.PACKAGE_RELEASE,
            compiler="clang-17",
            package_type="deb",
            static_binary_name="amd64",
            additional_pkgs=True,
        ),
        Build.PACKAGE_AARCH64: BuildConfig(
            name=Build.PACKAGE_AARCH64,
            compiler="clang-17-aarch64",
            package_type="deb",
            static_binary_name="aarch64",
            additional_pkgs=True,
        ),
        Build.PACKAGE_ASAN: BuildConfig(
            name=Build.PACKAGE_ASAN,
            compiler="clang-17",
            sanitizer="address",
            package_type="deb",
        ),
        Build.PACKAGE_UBSAN: BuildConfig(
            name=Build.PACKAGE_UBSAN,
            compiler="clang-17",
            sanitizer="undefined",
            package_type="deb",
        ),
        Build.PACKAGE_TSAN: BuildConfig(
            name=Build.PACKAGE_TSAN,
            compiler="clang-17",
            sanitizer="thread",
            package_type="deb",
        ),
        Build.PACKAGE_MSAN: BuildConfig(
            name=Build.PACKAGE_MSAN,
            compiler="clang-17",
            sanitizer="memory",
            package_type="deb",
        ),
        Build.PACKAGE_DEBUG: BuildConfig(
            name=Build.PACKAGE_DEBUG,
            compiler="clang-17",
            debug_build=True,
            package_type="deb",
            sparse_checkout=True,  # Check that it works with at least one build, see also update-submodules.sh
        ),
        Build.PACKAGE_RELEASE_COVERAGE: BuildConfig(
            name=Build.PACKAGE_RELEASE_COVERAGE,
            compiler="clang-17",
            coverage=True,
            package_type="deb",
        ),
        Build.BINARY_RELEASE: BuildConfig(
            name=Build.BINARY_RELEASE,
            compiler="clang-17",
            package_type="binary",
        ),
        Build.BINARY_TIDY: BuildConfig(
            name=Build.BINARY_TIDY,
            compiler="clang-17",
            debug_build=True,
            package_type="binary",
            static_binary_name="debug-amd64",
            tidy=True,
            comment="clang-tidy is used for static analysis",
        ),
        Build.BINARY_DARWIN: BuildConfig(
            name=Build.BINARY_DARWIN,
            compiler="clang-17-darwin",
            package_type="binary",
            static_binary_name="macos",
        ),
        Build.BINARY_AARCH64: BuildConfig(
            name=Build.BINARY_AARCH64,
            compiler="clang-17-aarch64",
            package_type="binary",
        ),
        Build.BINARY_AARCH64_V80COMPAT: BuildConfig(
            name=Build.BINARY_AARCH64_V80COMPAT,
            compiler="clang-17-aarch64-v80compat",
            package_type="binary",
            static_binary_name="aarch64v80compat",
            comment="For ARMv8.1 and older",
        ),
        Build.BINARY_FREEBSD: BuildConfig(
            name=Build.BINARY_FREEBSD,
            compiler="clang-17-freebsd",
            package_type="binary",
            static_binary_name="freebsd",
        ),
        Build.BINARY_DARWIN_AARCH64: BuildConfig(
            name=Build.BINARY_DARWIN_AARCH64,
            compiler="clang-17-darwin-aarch64",
            package_type="binary",
            static_binary_name="macos-aarch64",
        ),
        Build.BINARY_PPC64LE: BuildConfig(
            name=Build.BINARY_PPC64LE,
            compiler="clang-17-ppc64le",
            package_type="binary",
            static_binary_name="powerpc64le",
        ),
        Build.BINARY_AMD64_COMPAT: BuildConfig(
            name=Build.BINARY_AMD64_COMPAT,
            compiler="clang-17-amd64-compat",
            package_type="binary",
            static_binary_name="amd64compat",
            comment="SSE2-only build",
        ),
        Build.BINARY_AMD64_MUSL: BuildConfig(
            name=Build.BINARY_AMD64_MUSL,
            compiler="clang-17-amd64-musl",
            package_type="binary",
            static_binary_name="amd64musl",
            comment="Build with Musl",
        ),
        Build.BINARY_RISCV64: BuildConfig(
            name=Build.BINARY_RISCV64,
            compiler="clang-17-riscv64",
            package_type="binary",
            static_binary_name="riscv64",
        ),
        # disabled because s390x refused to build in the migration to OpenSSL
        # Build.BINARY_S390X: BuildConfig(
        #     name=Build.BINARY_S390X,
        #     compiler="clang-17-s390x",
        #     package_type="binary",
        #     static_binary_name="s390x",
        # ),
        Build.FUZZERS: BuildConfig(
            name=Build.FUZZERS,
            compiler="clang-17",
            package_type="fuzzers",
            job_config=fuzzer_build_job_config,
        ),
    },
    builds_report_config={
        JobNames.BUILD_CHECK: BuildReportConfig(
            builds=[
                Build.PACKAGE_RELEASE,
                Build.PACKAGE_AARCH64,
                Build.PACKAGE_ASAN,
                Build.PACKAGE_UBSAN,
                Build.PACKAGE_TSAN,
                Build.PACKAGE_MSAN,
                Build.PACKAGE_DEBUG,
            ]
        ),
        JobNames.BUILD_CHECK_SPECIAL: BuildReportConfig(
            builds=[
                Build.BINARY_TIDY,
                Build.BINARY_DARWIN,
                Build.BINARY_AARCH64,
                Build.BINARY_AARCH64_V80COMPAT,
                Build.BINARY_FREEBSD,
                Build.BINARY_DARWIN_AARCH64,
                Build.BINARY_PPC64LE,
                Build.BINARY_RISCV64,
                # Build.BINARY_S390X, # disabled because s390x refused to build in the migration to OpenSSL
                Build.BINARY_AMD64_COMPAT,
                Build.BINARY_AMD64_MUSL,
                Build.PACKAGE_RELEASE_COVERAGE,
                Build.BINARY_RELEASE,
                Build.FUZZERS,
            ]
        ),
    },
    other_jobs_configs={
        JobNames.DOCKER_SERVER: TestConfig("", job_config=docker_server_job_config),
        JobNames.DOCKER_KEEPER: TestConfig("", job_config=docker_server_job_config),
        JobNames.DOCS_CHECK: TestConfig(
            "",
            job_config=JobConfig(
                digest=DigestConfig(
                    include_paths=["**/*.md", "./docs", "tests/ci/docs_check.py"],
                    docker=["clickhouse/docs-builder"],
                ),
                run_command="docs_check.py",
            ),
        ),
        JobNames.FAST_TEST: TestConfig(
            "",
            job_config=JobConfig(
                pr_only=True,
                digest=DigestConfig(
                    include_paths=["./tests/queries/0_stateless/"],
                    exclude_files=[".md"],
                    docker=["clickhouse/fasttest"],
                ),
            ),
        ),
        JobNames.STYLE_CHECK: TestConfig(
            "",
            job_config=JobConfig(
                run_always=True,
            ),
        ),
        JobNames.BUGFIX_VALIDATE: TestConfig(
            "",
            # we run this check by label - no digest required
            job_config=JobConfig(
                run_by_label="pr-bugfix", run_command="bugfix_validate_check.py"
            ),
        ),
    },
    test_configs={
        JobNames.INSTALL_TEST_AMD: TestConfig(
            Build.PACKAGE_RELEASE, job_config=install_test_params
        ),
        JobNames.INSTALL_TEST_ARM: TestConfig(
            Build.PACKAGE_AARCH64, job_config=install_test_params
        ),
        JobNames.STATEFUL_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_RELEASE: TestConfig(
            Build.PACKAGE_RELEASE, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_RELEASE_COVERAGE: TestConfig(
            Build.PACKAGE_RELEASE_COVERAGE, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_AARCH64: TestConfig(
            Build.PACKAGE_AARCH64, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        # Stateful tests for parallel replicas
        JobNames.STATEFUL_TEST_PARALLEL_REPL_RELEASE: TestConfig(
            Build.PACKAGE_RELEASE, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_PARALLEL_REPL_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_PARALLEL_REPL_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(random_bucket="parrepl_with_sanitizer", **stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_PARALLEL_REPL_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(random_bucket="parrepl_with_sanitizer", **stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_PARALLEL_REPL_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN, job_config=JobConfig(random_bucket="parrepl_with_sanitizer", **stateful_test_common_params)  # type: ignore
        ),
        JobNames.STATEFUL_TEST_PARALLEL_REPL_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(random_bucket="parrepl_with_sanitizer", **stateful_test_common_params)  # type: ignore
        ),
        # End stateful tests for parallel replicas
        JobNames.STATELESS_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN,
            job_config=JobConfig(num_batches=4, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN,
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN,
            job_config=JobConfig(num_batches=6, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN,
            job_config=JobConfig(num_batches=2, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG,
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_RELEASE: TestConfig(
            Build.PACKAGE_RELEASE, job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        JobNames.STATELESS_TEST_RELEASE_COVERAGE: TestConfig(
            Build.PACKAGE_RELEASE_COVERAGE,
            job_config=JobConfig(num_batches=6, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_AARCH64: TestConfig(
            Build.PACKAGE_AARCH64, job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        JobNames.STATELESS_TEST_ANALYZER_S3_REPLICATED_RELEASE: TestConfig(
            Build.PACKAGE_RELEASE,
            job_config=JobConfig(num_batches=4, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_S3_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG,
            job_config=JobConfig(num_batches=6, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STATELESS_TEST_S3_TSAN: TestConfig(
            Build.PACKAGE_TSAN,
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        JobNames.STRESS_TEST_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        JobNames.STRESS_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        JobNames.STRESS_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(random_bucket="stress_with_sanitizer", **stress_test_common_params)  # type: ignore
        ),
        JobNames.STRESS_TEST_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN, job_config=JobConfig(random_bucket="stress_with_sanitizer", **stress_test_common_params)  # type: ignore
        ),
        JobNames.STRESS_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(random_bucket="stress_with_sanitizer", **stress_test_common_params)  # type: ignore
        ),
        JobNames.UPGRADE_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(pr_only=True, random_bucket="upgrade_with_sanitizer", **upgrade_test_common_params)  # type: ignore
        ),
        JobNames.UPGRADE_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(pr_only=True, random_bucket="upgrade_with_sanitizer", **upgrade_test_common_params)  # type: ignore
        ),
        JobNames.UPGRADE_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(pr_only=True, random_bucket="upgrade_with_sanitizer", **upgrade_test_common_params)  # type: ignore
        ),
        JobNames.UPGRADE_TEST_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=JobConfig(pr_only=True, **upgrade_test_common_params)  # type: ignore
        ),
        JobNames.INTEGRATION_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN,
            job_config=JobConfig(num_batches=4, **integration_test_common_params, release_only=True),  # type: ignore
        ),
        JobNames.INTEGRATION_TEST_ASAN_ANALYZER: TestConfig(
            Build.PACKAGE_ASAN,
            job_config=JobConfig(num_batches=6, **integration_test_common_params),  # type: ignore
        ),
        JobNames.INTEGRATION_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN,
            job_config=JobConfig(num_batches=6, **integration_test_common_params),  # type: ignore
        ),
        JobNames.INTEGRATION_TEST_ARM: TestConfig(
            Build.PACKAGE_AARCH64,
            # add [run_by_label="test arm"] to not run in regular pr workflow by default
            job_config=JobConfig(num_batches=6, **integration_test_common_params, run_by_label="test arm"),  # type: ignore
        ),
        JobNames.INTEGRATION_TEST: TestConfig(
            Build.PACKAGE_RELEASE,
            job_config=JobConfig(num_batches=4, **integration_test_common_params, release_only=True),  # type: ignore
        ),
        JobNames.INTEGRATION_TEST_FLAKY: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(pr_only=True, **integration_test_common_params)  # type: ignore
        ),
        JobNames.COMPATIBILITY_TEST: TestConfig(
            Build.PACKAGE_RELEASE,
            job_config=JobConfig(
                required_on_release_branch=True, **compatibility_test_common_params  # type: ignore
            ),
        ),
        JobNames.COMPATIBILITY_TEST_ARM: TestConfig(
            Build.PACKAGE_AARCH64,
            job_config=JobConfig(
                required_on_release_branch=True, **compatibility_test_common_params  # type: ignore
            ),
        ),
        JobNames.UNIT_TEST: TestConfig(
            Build.BINARY_RELEASE, job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        JobNames.UNIT_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        JobNames.UNIT_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        JobNames.UNIT_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        JobNames.UNIT_TEST_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN, job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        JobNames.AST_FUZZER_TEST_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        JobNames.AST_FUZZER_TEST_ASAN: TestConfig(
            Build.PACKAGE_ASAN, job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        JobNames.AST_FUZZER_TEST_MSAN: TestConfig(
            Build.PACKAGE_MSAN, job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        JobNames.AST_FUZZER_TEST_TSAN: TestConfig(
            Build.PACKAGE_TSAN, job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        JobNames.AST_FUZZER_TEST_UBSAN: TestConfig(
            Build.PACKAGE_UBSAN, job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        JobNames.STATELESS_TEST_FLAKY_ASAN: TestConfig(
            # replace to non-default
            Build.PACKAGE_ASAN,
            job_config=JobConfig(pr_only=True, **{**statless_test_common_params, "timeout": 3600}),  # type: ignore
        ),
        JobNames.JEPSEN_KEEPER: TestConfig(
            Build.BINARY_RELEASE,
            job_config=JobConfig(
                run_by_label="jepsen-test", run_command="jepsen_check.py keeper"
            ),
        ),
        JobNames.JEPSEN_SERVER: TestConfig(
            Build.BINARY_RELEASE,
            job_config=JobConfig(
                run_by_label="jepsen-test", run_command="jepsen_check.py server"
            ),
        ),
        JobNames.PERFORMANCE_TEST_AMD64: TestConfig(
            Build.PACKAGE_RELEASE,
            job_config=JobConfig(num_batches=4, **perf_test_common_params),  # type: ignore
        ),
        JobNames.PERFORMANCE_TEST_ARM64: TestConfig(
            Build.PACKAGE_AARCH64,
            job_config=JobConfig(num_batches=4, run_by_label="pr-performance", **perf_test_common_params),  # type: ignore
        ),
        JobNames.SQLANCER: TestConfig(
            Build.PACKAGE_RELEASE, job_config=sqllancer_test_common_params
        ),
        JobNames.SQLANCER_DEBUG: TestConfig(
            Build.PACKAGE_DEBUG, job_config=sqllancer_test_common_params
        ),
        JobNames.SQL_LOGIC_TEST: TestConfig(
            Build.PACKAGE_RELEASE, job_config=sqllogic_test_params
        ),
        JobNames.SQLTEST: TestConfig(Build.PACKAGE_RELEASE, job_config=sql_test_params),
        JobNames.CLCIKBENCH_TEST: TestConfig(
            Build.PACKAGE_RELEASE, job_config=JobConfig(**clickbench_test_params)  # type: ignore
        ),
        JobNames.CLCIKBENCH_TEST_ARM: TestConfig(
            Build.PACKAGE_AARCH64, job_config=JobConfig(**clickbench_test_params)  # type: ignore
        ),
        JobNames.LIBFUZZER_TEST: TestConfig(
            Build.FUZZERS,
            job_config=JobConfig(
                run_by_label=Labels.libFuzzer,
                timeout=10800,
                run_command='libfuzzer_test_check.py "$CHECK_NAME" 10800',
            ),
        ),  # type: ignore
    },
)
CI_CONFIG.validate()


# checks required by Mergeable Check
REQUIRED_CHECKS = [
    "PR Check",
    StatusNames.SYNC,
    JobNames.BUILD_CHECK,
    JobNames.BUILD_CHECK_SPECIAL,
    JobNames.DOCS_CHECK,
    JobNames.FAST_TEST,
    JobNames.STATEFUL_TEST_RELEASE,
    JobNames.STATELESS_TEST_RELEASE,
    JobNames.STYLE_CHECK,
    JobNames.UNIT_TEST_ASAN,
    JobNames.UNIT_TEST_MSAN,
    JobNames.UNIT_TEST,
    JobNames.UNIT_TEST_TSAN,
    JobNames.UNIT_TEST_UBSAN,
    JobNames.INTEGRATION_TEST_ASAN_ANALYZER,
    JobNames.STATELESS_TEST_ANALYZER_S3_REPLICATED_RELEASE,
]


@dataclass
class CheckDescription:
    name: str
    description: str  # the check descriptions, will be put into the status table
    match_func: Callable[[str], bool]  # the function to check vs the commit status

    def __hash__(self) -> int:
        return hash(self.name + self.description)


CHECK_DESCRIPTIONS = [
    CheckDescription(
        "AST fuzzer",
        "Runs randomly generated queries to catch program errors. "
        "The build type is optionally given in parenthesis. "
        "If it fails, ask a maintainer for help",
        lambda x: x.startswith("AST fuzzer"),
    ),
    CheckDescription(
        JobNames.BUGFIX_VALIDATE,
        "Checks that either a new test (functional or integration) or there "
        "some changed tests that fail with the binary built on master branch",
        lambda x: x == JobNames.BUGFIX_VALIDATE,
    ),
    CheckDescription(
        "CI running",
        "A meta-check that indicates the running CI. Normally, it's in <b>success</b> or "
        "<b>pending</b> state. The failed status indicates some problems with the PR",
        lambda x: x == "CI running",
    ),
    CheckDescription(
        "ClickHouse build check",
        "Builds ClickHouse in various configurations for use in further steps. "
        "You have to fix the builds that fail. Build logs often has enough "
        "information to fix the error, but you might have to reproduce the failure "
        "locally. The <b>cmake</b> options can be found in the build log, grepping for "
        '<b>cmake</b>. Use these options and follow the <a href="'
        'https://clickhouse.com/docs/en/development/build">general build process</a>',
        lambda x: x.startswith("ClickHouse") and x.endswith("build check"),
    ),
    CheckDescription(
        "Compatibility check",
        "Checks that <b>clickhouse</b> binary runs on distributions with old libc "
        "versions. If it fails, ask a maintainer for help",
        lambda x: x.startswith("Compatibility check"),
    ),
    CheckDescription(
        JobNames.DOCKER_SERVER,
        "The check to build and optionally push the mentioned image to docker hub",
        lambda x: x.startswith("Docker server"),
    ),
    CheckDescription(
        JobNames.DOCKER_KEEPER,
        "The check to build and optionally push the mentioned image to docker hub",
        lambda x: x.startswith("Docker keeper"),
    ),
    CheckDescription(
        JobNames.DOCS_CHECK,
        "Builds and tests the documentation",
        lambda x: x == JobNames.DOCS_CHECK,
    ),
    CheckDescription(
        JobNames.FAST_TEST,
        "Normally this is the first check that is ran for a PR. It builds ClickHouse "
        'and runs most of <a href="https://clickhouse.com/docs/en/development/tests'
        '#functional-tests">stateless functional tests</a>, '
        "omitting some. If it fails, further checks are not started until it is fixed. "
        "Look at the report to see which tests fail, then reproduce the failure "
        'locally as described <a href="https://clickhouse.com/docs/en/development/'
        'tests#functional-test-locally">here</a>',
        lambda x: x == JobNames.FAST_TEST,
    ),
    CheckDescription(
        "Flaky tests",
        "Checks if new added or modified tests are flaky by running them repeatedly, "
        "in parallel, with more randomization. Functional tests are run 100 times "
        "with address sanitizer, and additional randomization of thread scheduling. "
        "Integrational tests are run up to 10 times. If at least once a new test has "
        "failed, or was too long, this check will be red. We don't allow flaky tests, "
        'read <a href="https://clickhouse.com/blog/decorating-a-christmas-tree-with-'
        'the-help-of-flaky-tests/">the doc</a>',
        lambda x: "tests flaky check" in x,
    ),
    CheckDescription(
        "Install packages",
        "Checks that the built packages are installable in a clear environment",
        lambda x: x.startswith("Install packages ("),
    ),
    CheckDescription(
        "Integration tests",
        "The integration tests report. In parenthesis the package type is given, "
        "and in square brackets are the optional part/total tests",
        lambda x: x.startswith("Integration tests ("),
    ),
    CheckDescription(
        StatusNames.MERGEABLE,
        "Checks if all other necessary checks are successful",
        lambda x: x == StatusNames.MERGEABLE,
    ),
    CheckDescription(
        "Performance Comparison",
        "Measure changes in query performance. The performance test report is "
        'described in detail <a href="https://github.com/ClickHouse/ClickHouse/tree'
        '/master/docker/test/performance-comparison#how-to-read-the-report">here</a>. '
        "In square brackets are the optional part/total tests",
        lambda x: x.startswith("Performance Comparison"),
    ),
    CheckDescription(
        "Push to Dockerhub",
        "The check for building and pushing the CI related docker images to docker hub",
        lambda x: x.startswith("Push") and "to Dockerhub" in x,
    ),
    CheckDescription(
        "Sqllogic",
        "Run clickhouse on the "
        '<a href="https://www.sqlite.org/sqllogictest">sqllogic</a> '
        "test set against sqlite and checks that all statements are passed",
        lambda x: x.startswith("Sqllogic test"),
    ),
    CheckDescription(
        "SQLancer",
        "Fuzzing tests that detect logical bugs with "
        '<a href="https://github.com/sqlancer/sqlancer">SQLancer</a> tool',
        lambda x: x.startswith("SQLancer"),
    ),
    CheckDescription(
        "Stateful tests",
        "Runs stateful functional tests for ClickHouse binaries built in various "
        "configurations -- release, debug, with sanitizers, etc",
        lambda x: x.startswith("Stateful tests ("),
    ),
    CheckDescription(
        "Stateless tests",
        "Runs stateless functional tests for ClickHouse binaries built in various "
        "configurations -- release, debug, with sanitizers, etc",
        lambda x: x.startswith("Stateless tests ("),
    ),
    CheckDescription(
        "Stress test",
        "Runs stateless functional tests concurrently from several clients to detect "
        "concurrency-related errors",
        lambda x: x.startswith("Stress test ("),
    ),
    CheckDescription(
        JobNames.STYLE_CHECK,
        "Runs a set of checks to keep the code style clean. If some of tests failed, "
        "see the related log from the report",
        lambda x: x == JobNames.STYLE_CHECK,
    ),
    CheckDescription(
        "Unit tests",
        "Runs the unit tests for different release types",
        lambda x: x.startswith("Unit tests ("),
    ),
    CheckDescription(
        "Upgrade check",
        "Runs stress tests on server version from last release and then tries to "
        "upgrade it to the version from the PR. It checks if the new server can "
        "successfully startup without any errors, crashes or sanitizer asserts",
        lambda x: x.startswith("Upgrade check ("),
    ),
    CheckDescription(
        "ClickBench",
        "Runs [ClickBench](https://github.com/ClickHouse/ClickBench/) with instant-attach table",
        lambda x: x.startswith("ClickBench"),
    ),
    CheckDescription(
        "Falback for unknown",
        "There's no description for the check yet, please add it to "
        "tests/ci/ci_config.py:CHECK_DESCRIPTIONS",
        lambda x: True,
    ),
]


def main() -> None:
    parser = ArgumentParser(
        formatter_class=ArgumentDefaultsHelpFormatter,
        description="The script provides build config for GITHUB_ENV or shell export",
    )
    parser.add_argument("--build-name", help="the build config to export")
    parser.add_argument(
        "--export",
        action="store_true",
        help="if set, the ENV parameters are provided for shell export",
    )
    args = parser.parse_args()
    build_config = CI_CONFIG.build_config.get(args.build_name)
    if build_config:
        print(build_config.export_env(args.export))


if __name__ == "__main__":
    main()
