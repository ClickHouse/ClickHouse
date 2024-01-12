#!/usr/bin/env python3

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Literal, Optional, Union

from integration_test_images import IMAGES


class Labels(Enum):
    DO_NOT_TEST_LABEL = "do not test"


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
    class to configure different CI scenarious per GH label
    """

    run_jobs: Iterable[str] = frozenset()


@dataclass
class JobConfig:
    """
    contains config parameter relevant for job execution in CI workflow
    @digest - configures digest calculation for the job
    @run_command - will be triggered for the job if omited in CI workflow yml
    @timeout
    @num_batches - sets number of batches for multi-batch job
    """

    digest: DigestConfig = field(default_factory=DigestConfig)
    run_command: str = ""
    timeout: Optional[int] = None
    num_batches: int = 1
    run_by_label: str = ""
    run_always: bool = False


@dataclass
class BuildConfig:
    name: str
    compiler: str
    package_type: Literal["deb", "binary", "fuzzers"]
    additional_pkgs: bool = False
    debug_build: bool = False
    sanitizer: str = ""
    tidy: bool = False
    sparse_checkout: bool = False
    comment: str = ""
    static_binary_name: str = ""
    job_config: JobConfig = field(
        default_factory=lambda: JobConfig(
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
                    "./cmake",
                    "./base",
                    "./programs",
                    "./packages",
                    "./docker/packager/packager",
                ],
                exclude_files=[".md"],
                docker=["clickhouse/binary-builder"],
                git_submodules=True,
            ),
        )
    )

    def export_env(self, export: bool = False) -> str:
        def process(field_name: str, field: Union[bool, str]) -> str:
            if isinstance(field, bool):
                field = str(field).lower()
            if export:
                return f"export BUILD_{field_name.upper()}={repr(field)}"
            return f"BUILD_{field_name.upper()}={field}"

        return "\n".join(process(k, v) for k, v in self.__dict__.items())


@dataclass
class BuildReportConfig:
    builds: List[str]
    job_config: JobConfig = field(default_factory=JobConfig)


@dataclass
class TestConfig:
    required_build: str
    force_tests: bool = False
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
statless_check_digest = DigestConfig(
    include_paths=["./tests/queries/0_stateless/"],
    exclude_files=[".md"],
    docker=["clickhouse/stateless-test"],
)
stateful_check_digest = DigestConfig(
    include_paths=["./tests/queries/1_stateful/"],
    exclude_files=[".md"],
    docker=["clickhouse/stateful-test"],
)
# FIXME: which tests are stresstest? stateless?
stress_check_digest = DigestConfig(
    include_paths=["./tests/queries/0_stateless/"],
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
    include_paths=["./tests/ci/integration_test_check.py", "./tests/integration"],
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
statless_test_common_params = {
    "digest": statless_check_digest,
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
sqllancer_test_common_params = {
    "digest": sqllancer_check_digest,
    "run_command": "sqlancer_check.py",
    "run_always": True,
}
sqllogic_test_params = {
    "digest": sqllogic_check_digest,
    "run_command": "sqllogic_test.py",
    "timeout": 10800,
}
sql_test_params = {
    "digest": sqltest_check_digest,
    "run_command": "sqltest.py",
    "timeout": 10800,
}


@dataclass
class CiConfig:
    """
    Contains configs for ALL jobs in CI pipeline
    each config item in the below dicts should be an instance of JobConfig class or inherited from it
    """

    build_config: BuildConfigs
    builds_report_config: BuildsReportConfig
    test_configs: TestConfigs
    other_jobs_configs: TestConfigs
    label_configs: LabelConfigs

    def get_label_config(self, label_name: str) -> Optional[LabelConfig]:
        for label, config in self.label_configs.items():
            if label_name == label:
                return config
        return None

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
        assert (
            res is not None
        ), f"Invalid check_name or CI_CONFIG outdated, config not found for [{check_name}]"
        return res  # type: ignore

    def get_job_with_parents(self, check_name: str) -> List[str]:
        def _normalize_string(input_string: str) -> str:
            lowercase_string = input_string.lower()
            normalized_string = (
                lowercase_string.replace(" ", "_")
                .replace("-", "_")
                .replace("(", "")
                .replace(")", "")
                .replace(",", "")
            )
            return normalized_string

        res = []
        check_name = _normalize_string(check_name)

        for config in (
            self.build_config,
            self.builds_report_config,
            self.test_configs,
            self.other_jobs_configs,
        ):
            for job_name in config:  # type: ignore
                if check_name == _normalize_string(job_name):
                    res.append(job_name)
                    if isinstance(config[job_name], TestConfig):  # type: ignore
                        assert config[
                            job_name
                        ].required_build, f"Error: Experimantal feature... Not supported job [{job_name}]"  # type: ignore
                        res.append(config[job_name].required_build)  # type: ignore
                        res.append("Fast tests")
                        res.append("Style check")
                    elif isinstance(config[job_name], BuildConfig):  # type: ignore
                        res.append("Fast tests")
                        res.append("Style check")
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

    def job_generator(self) -> Iterable[str]:
        """
        traverses all check names in CI pipeline
        """
        for config in (
            self.other_jobs_configs,
            self.build_config,
            self.builds_report_config,
            self.test_configs,
        ):
            for check_name in config:  # type: ignore
                yield check_name

    def get_builds_for_report(self, report_name: str) -> List[str]:
        return self.builds_report_config[report_name].builds

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


CI_CONFIG = CiConfig(
    label_configs={
        Labels.DO_NOT_TEST_LABEL.value: LabelConfig(run_jobs=["Style check"]),
    },
    build_config={
        "package_release": BuildConfig(
            name="package_release",
            compiler="clang-17",
            package_type="deb",
            static_binary_name="amd64",
            additional_pkgs=True,
        ),
        "package_aarch64": BuildConfig(
            name="package_aarch64",
            compiler="clang-17-aarch64",
            package_type="deb",
            static_binary_name="aarch64",
            additional_pkgs=True,
        ),
        "package_asan": BuildConfig(
            name="package_asan",
            compiler="clang-17",
            sanitizer="address",
            package_type="deb",
        ),
        "package_ubsan": BuildConfig(
            name="package_ubsan",
            compiler="clang-17",
            sanitizer="undefined",
            package_type="deb",
        ),
        "package_tsan": BuildConfig(
            name="package_tsan",
            compiler="clang-17",
            sanitizer="thread",
            package_type="deb",
        ),
        "package_msan": BuildConfig(
            name="package_msan",
            compiler="clang-17",
            sanitizer="memory",
            package_type="deb",
        ),
        "package_debug": BuildConfig(
            name="package_debug",
            compiler="clang-17",
            debug_build=True,
            package_type="deb",
            sparse_checkout=True,
        ),
        "binary_release": BuildConfig(
            name="binary_release",
            compiler="clang-17",
            package_type="binary",
        ),
        "binary_tidy": BuildConfig(
            name="binary_tidy",
            compiler="clang-17",
            debug_build=True,
            package_type="binary",
            static_binary_name="debug-amd64",
            tidy=True,
            comment="clang-tidy is used for static analysis",
        ),
        "binary_darwin": BuildConfig(
            name="binary_darwin",
            compiler="clang-17-darwin",
            package_type="binary",
            static_binary_name="macos",
            sparse_checkout=True,
        ),
        "binary_aarch64": BuildConfig(
            name="binary_aarch64",
            compiler="clang-17-aarch64",
            package_type="binary",
        ),
        "binary_aarch64_v80compat": BuildConfig(
            name="binary_aarch64_v80compat",
            compiler="clang-17-aarch64-v80compat",
            package_type="binary",
            static_binary_name="aarch64v80compat",
            comment="For ARMv8.1 and older",
        ),
        "binary_freebsd": BuildConfig(
            name="binary_freebsd",
            compiler="clang-17-freebsd",
            package_type="binary",
            static_binary_name="freebsd",
        ),
        "binary_darwin_aarch64": BuildConfig(
            name="binary_darwin_aarch64",
            compiler="clang-17-darwin-aarch64",
            package_type="binary",
            static_binary_name="macos-aarch64",
        ),
        "binary_ppc64le": BuildConfig(
            name="binary_ppc64le",
            compiler="clang-17-ppc64le",
            package_type="binary",
            static_binary_name="powerpc64le",
        ),
        "binary_amd64_compat": BuildConfig(
            name="binary_amd64_compat",
            compiler="clang-17-amd64-compat",
            package_type="binary",
            static_binary_name="amd64compat",
            comment="SSE2-only build",
        ),
        "binary_amd64_musl": BuildConfig(
            name="binary_amd64_musl",
            compiler="clang-17-amd64-musl",
            package_type="binary",
            static_binary_name="amd64musl",
            comment="Build with Musl",
        ),
        "binary_riscv64": BuildConfig(
            name="binary_riscv64",
            compiler="clang-17-riscv64",
            package_type="binary",
            static_binary_name="riscv64",
        ),
        "binary_s390x": BuildConfig(
            name="binary_s390x",
            compiler="clang-17-s390x",
            package_type="binary",
            static_binary_name="s390x",
        ),
        "fuzzers": BuildConfig(
            name="fuzzers",
            compiler="clang-17",
            package_type="fuzzers",
        ),
    },
    builds_report_config={
        "ClickHouse build check": BuildReportConfig(
            builds=[
                "package_release",
                "package_aarch64",
                "package_asan",
                "package_ubsan",
                "package_tsan",
                "package_msan",
                "package_debug",
                "binary_release",
                "fuzzers",
            ]
        ),
        "ClickHouse special build check": BuildReportConfig(
            builds=[
                "binary_tidy",
                "binary_darwin",
                "binary_aarch64",
                "binary_aarch64_v80compat",
                "binary_freebsd",
                "binary_darwin_aarch64",
                "binary_ppc64le",
                "binary_riscv64",
                "binary_s390x",
                "binary_amd64_compat",
                "binary_amd64_musl",
            ]
        ),
    },
    other_jobs_configs={
        "Docker server and keeper images": TestConfig(
            "",
            job_config=JobConfig(
                digest=DigestConfig(
                    include_paths=[
                        "tests/ci/docker_server.py",
                        "./docker/server",
                        "./docker/keeper",
                    ]
                )
            ),
        ),
        "Docs check": TestConfig(
            "",
            job_config=JobConfig(
                digest=DigestConfig(
                    include_paths=["**/*.md", "./docs", "tests/ci/docs_check.py"],
                    docker=["clickhouse/docs-builder"],
                ),
            ),
        ),
        "Fast tests": TestConfig(
            "",
            job_config=JobConfig(
                digest=DigestConfig(
                    include_paths=["./tests/queries/0_stateless/"],
                    exclude_files=[".md"],
                    docker=["clickhouse/fasttest"],
                )
            ),
        ),
        "Style check": TestConfig(
            "",
            job_config=JobConfig(
                run_always=True,
            ),
        ),
        "tests bugfix validate check": TestConfig(
            "",
            # we run this check by label - no digest required
            job_config=JobConfig(run_by_label="pr-bugfix"),
        ),
    },
    test_configs={
        "Install packages (amd64)": TestConfig(
            "package_release", job_config=JobConfig(digest=install_check_digest)
        ),
        "Install packages (arm64)": TestConfig(
            "package_aarch64", job_config=JobConfig(digest=install_check_digest)
        ),
        "Stateful tests (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (tsan)": TestConfig(
            "package_tsan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (msan)": TestConfig(
            "package_msan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (ubsan)": TestConfig(
            "package_ubsan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (debug)": TestConfig(
            "package_debug", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (release)": TestConfig(
            "package_release", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (aarch64)": TestConfig(
            "package_aarch64", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (release, DatabaseOrdinary)": TestConfig(
            "package_release", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        # "Stateful tests (release, DatabaseReplicated)": TestConfig(
        #     "package_release", job_config=JobConfig(**stateful_test_common_params) # type: ignore
        # ),
        # Stateful tests for parallel replicas
        "Stateful tests (release, ParallelReplicas)": TestConfig(
            "package_release", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (debug, ParallelReplicas)": TestConfig(
            "package_debug", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (asan, ParallelReplicas)": TestConfig(
            "package_asan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (msan, ParallelReplicas)": TestConfig(
            "package_msan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (ubsan, ParallelReplicas)": TestConfig(
            "package_ubsan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        "Stateful tests (tsan, ParallelReplicas)": TestConfig(
            "package_tsan", job_config=JobConfig(**stateful_test_common_params)  # type: ignore
        ),
        # End stateful tests for parallel replicas
        "Stateless tests (asan)": TestConfig(
            "package_asan",
            job_config=JobConfig(num_batches=4, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (tsan)": TestConfig(
            "package_tsan",
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (msan)": TestConfig(
            "package_msan",
            job_config=JobConfig(num_batches=6, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (ubsan)": TestConfig(
            "package_ubsan",
            job_config=JobConfig(num_batches=2, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (debug)": TestConfig(
            "package_debug",
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (release)": TestConfig(
            "package_release", job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        "Stateless tests (aarch64)": TestConfig(
            "package_aarch64", job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        "Stateless tests (release, analyzer)": TestConfig(
            "package_release", job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        "Stateless tests (release, DatabaseOrdinary)": TestConfig(
            "package_release", job_config=JobConfig(**statless_test_common_params)  # type: ignore
        ),
        "Stateless tests (release, DatabaseReplicated)": TestConfig(
            "package_release",
            job_config=JobConfig(num_batches=4, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (release, s3 storage)": TestConfig(
            "package_release",
            job_config=JobConfig(num_batches=2, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (debug, s3 storage)": TestConfig(
            "package_debug",
            job_config=JobConfig(num_batches=6, **statless_test_common_params),  # type: ignore
        ),
        "Stateless tests (tsan, s3 storage)": TestConfig(
            "package_tsan",
            job_config=JobConfig(num_batches=5, **statless_test_common_params),  # type: ignore
        ),
        "Stress test (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        "Stress test (tsan)": TestConfig(
            "package_tsan", job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        "Stress test (ubsan)": TestConfig(
            "package_ubsan", job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        "Stress test (msan)": TestConfig(
            "package_msan", job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        "Stress test (debug)": TestConfig(
            "package_debug", job_config=JobConfig(**stress_test_common_params)  # type: ignore
        ),
        "Upgrade check (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**upgrade_test_common_params)  # type: ignore
        ),
        "Upgrade check (tsan)": TestConfig(
            "package_tsan", job_config=JobConfig(**upgrade_test_common_params)  # type: ignore
        ),
        "Upgrade check (msan)": TestConfig(
            "package_msan", job_config=JobConfig(**upgrade_test_common_params)  # type: ignore
        ),
        "Upgrade check (debug)": TestConfig(
            "package_debug", job_config=JobConfig(**upgrade_test_common_params)  # type: ignore
        ),
        "Integration tests (asan)": TestConfig(
            "package_asan",
            job_config=JobConfig(num_batches=4, **integration_test_common_params),  # type: ignore
        ),
        "Integration tests (asan, analyzer)": TestConfig(
            "package_asan",
            job_config=JobConfig(num_batches=6, **integration_test_common_params),  # type: ignore
        ),
        "Integration tests (tsan)": TestConfig(
            "package_tsan",
            job_config=JobConfig(num_batches=6, **integration_test_common_params),  # type: ignore
        ),
        # FIXME: currently no wf has this job. Try to enable
        # "Integration tests (msan)": TestConfig("package_msan", job_config=JobConfig(num_batches=6, **integration_test_common_params) # type: ignore
        # ),
        "Integration tests (release)": TestConfig(
            "package_release",
            job_config=JobConfig(num_batches=4, **integration_test_common_params),  # type: ignore
        ),
        "Integration tests flaky check (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**integration_test_common_params)  # type: ignore
        ),
        "Compatibility check (amd64)": TestConfig(
            "package_release", job_config=JobConfig(digest=compatibility_check_digest)
        ),
        "Compatibility check (aarch64)": TestConfig(
            "package_aarch64", job_config=JobConfig(digest=compatibility_check_digest)
        ),
        "Unit tests (release)": TestConfig(
            "binary_release", job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        "Unit tests (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        "Unit tests (msan)": TestConfig(
            "package_msan", job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        "Unit tests (tsan)": TestConfig(
            "package_tsan", job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        "Unit tests (ubsan)": TestConfig(
            "package_ubsan", job_config=JobConfig(**unit_test_common_params)  # type: ignore
        ),
        "AST fuzzer (debug)": TestConfig(
            "package_debug", job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        "AST fuzzer (asan)": TestConfig(
            "package_asan", job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        "AST fuzzer (msan)": TestConfig(
            "package_msan", job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        "AST fuzzer (tsan)": TestConfig(
            "package_tsan", job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        "AST fuzzer (ubsan)": TestConfig(
            "package_ubsan", job_config=JobConfig(**astfuzzer_test_common_params)  # type: ignore
        ),
        "Stateless tests flaky check (asan)": TestConfig(
            # replace to non-default
            "package_asan",
            job_config=JobConfig(**{**statless_test_common_params, "timeout": 3600}),  # type: ignore
        ),
        # FIXME: add digest and params
        "ClickHouse Keeper Jepsen": TestConfig("binary_release"),
        # FIXME: add digest and params
        "ClickHouse Server Jepsen": TestConfig("binary_release"),
        "Performance Comparison": TestConfig(
            "package_release",
            job_config=JobConfig(num_batches=4, **perf_test_common_params),  # type: ignore
        ),
        "Performance Comparison Aarch64": TestConfig(
            "package_aarch64",
            job_config=JobConfig(num_batches=4, run_by_label="pr-performance", **perf_test_common_params),  # type: ignore
        ),
        "SQLancer (release)": TestConfig(
            "package_release", job_config=JobConfig(**sqllancer_test_common_params)  # type: ignore
        ),
        "SQLancer (debug)": TestConfig(
            "package_debug", job_config=JobConfig(**sqllancer_test_common_params)  # type: ignore
        ),
        "Sqllogic test (release)": TestConfig(
            "package_release", job_config=JobConfig(**sqllogic_test_params)  # type: ignore
        ),
        "SQLTest": TestConfig(
            "package_release", job_config=JobConfig(**sql_test_params)  # type: ignore
        ),
        "ClickBench (amd64)": TestConfig("package_release"),
        "ClickBench (aarch64)": TestConfig("package_aarch64"),
        # FIXME: add digest and params
        "libFuzzer tests": TestConfig("fuzzers"),  # type: ignore
    },
)
CI_CONFIG.validate()


# checks required by Mergeable Check
REQUIRED_CHECKS = [
    "PR Check",
    "ClickHouse build check",
    "ClickHouse special build check",
    "Docs Check",
    "Fast test",
    "Stateful tests (release)",
    "Stateless tests (release)",
    "Style Check",
    "Unit tests (asan)",
    "Unit tests (msan)",
    "Unit tests (release)",
    "Unit tests (tsan)",
    "Unit tests (ubsan)",
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
        "Bugfix validate check",
        "Checks that either a new test (functional or integration) or there "
        "some changed tests that fail with the binary built on master branch",
        lambda x: x == "Bugfix validate check",
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
        "Docker image for servers",
        "The check to build and optionally push the mentioned image to docker hub",
        lambda x: x.startswith("Docker image")
        and (x.endswith("building check") or x.endswith("build and push")),
    ),
    CheckDescription(
        "Docs Check", "Builds and tests the documentation", lambda x: x == "Docs Check"
    ),
    CheckDescription(
        "Fast test",
        "Normally this is the first check that is ran for a PR. It builds ClickHouse "
        'and runs most of <a href="https://clickhouse.com/docs/en/development/tests'
        '#functional-tests">stateless functional tests</a>, '
        "omitting some. If it fails, further checks are not started until it is fixed. "
        "Look at the report to see which tests fail, then reproduce the failure "
        'locally as described <a href="https://clickhouse.com/docs/en/development/'
        'tests#functional-test-locally">here</a>',
        lambda x: x == "Fast test",
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
        "Mergeable Check",
        "Checks if all other necessary checks are successful",
        lambda x: x == "Mergeable Check",
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
        "Style Check",
        "Runs a set of checks to keep the code style clean. If some of tests failed, "
        "see the related log from the report",
        lambda x: x == "Style Check",
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
