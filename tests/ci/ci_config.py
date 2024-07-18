#!/usr/bin/env python3

import logging

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from dataclasses import dataclass
from typing import Callable, Dict, List, Literal, Union


@dataclass
class BuildConfig:
    name: str
    compiler: str
    package_type: Literal["deb", "binary"]
    additional_pkgs: bool = False
    debug_build: bool = False
    sanitizer: str = ""
    tidy: bool = False
    sparse_checkout: bool = False
    comment: str = ""
    static_binary_name: str = ""

    def export_env(self, export: bool = False) -> str:
        def process(field_name: str, field: Union[bool, str]) -> str:
            if isinstance(field, bool):
                field = str(field).lower()
            if export:
                return f"export BUILD_{field_name.upper()}={repr(field)}"
            return f"BUILD_{field_name.upper()}={field}"

        return "\n".join(process(k, v) for k, v in self.__dict__.items())


@dataclass
class TestConfig:
    required_build: str
    force_tests: bool = False


BuildConfigs = Dict[str, BuildConfig]
BuildsReportConfig = Dict[str, List[str]]
TestConfigs = Dict[str, TestConfig]


@dataclass
class CiConfig:
    build_config: BuildConfigs
    builds_report_config: BuildsReportConfig
    test_configs: TestConfigs

    def validate(self) -> None:
        errors = []
        for name, build_config in self.build_config.items():
            build_in_reports = False
            for report_config in self.builds_report_config.values():
                if name in report_config:
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
        for build_report_name, build_names in self.builds_report_config.items():
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
    build_config={
        "package_release": BuildConfig(
            name="package_release",
            compiler="clang-16",
            package_type="deb",
            static_binary_name="amd64",
            additional_pkgs=True,
        ),
        "package_aarch64": BuildConfig(
            name="package_aarch64",
            compiler="clang-16-aarch64",
            package_type="deb",
            static_binary_name="aarch64",
            additional_pkgs=True,
        ),
        "package_asan": BuildConfig(
            name="package_asan",
            compiler="clang-16",
            sanitizer="address",
            package_type="deb",
        ),
        "package_ubsan": BuildConfig(
            name="package_ubsan",
            compiler="clang-16",
            sanitizer="undefined",
            package_type="deb",
        ),
        "package_tsan": BuildConfig(
            name="package_tsan",
            compiler="clang-16",
            sanitizer="thread",
            package_type="deb",
        ),
        "package_msan": BuildConfig(
            name="package_msan",
            compiler="clang-16",
            sanitizer="memory",
            package_type="deb",
        ),
        "package_debug": BuildConfig(
            name="package_debug",
            compiler="clang-16",
            debug_build=True,
            package_type="deb",
            sparse_checkout=True,
        ),
        "binary_release": BuildConfig(
            name="binary_release",
            compiler="clang-16",
            package_type="binary",
        ),
        "binary_tidy": BuildConfig(
            name="binary_tidy",
            compiler="clang-16",
            debug_build=True,
            package_type="binary",
            static_binary_name="debug-amd64",
            tidy=True,
            comment="clang-tidy is used for static analysis",
        ),
        "binary_darwin": BuildConfig(
            name="binary_darwin",
            compiler="clang-16-darwin",
            package_type="binary",
            static_binary_name="macos",
            sparse_checkout=True,
        ),
        "binary_aarch64": BuildConfig(
            name="binary_aarch64",
            compiler="clang-16-aarch64",
            package_type="binary",
        ),
        "binary_aarch64_v80compat": BuildConfig(
            name="binary_aarch64_v80compat",
            compiler="clang-16-aarch64-v80compat",
            package_type="binary",
            static_binary_name="aarch64v80compat",
            comment="For ARMv8.1 and older",
        ),
        "binary_freebsd": BuildConfig(
            name="binary_freebsd",
            compiler="clang-16-freebsd",
            package_type="binary",
            static_binary_name="freebsd",
        ),
        "binary_darwin_aarch64": BuildConfig(
            name="binary_darwin_aarch64",
            compiler="clang-16-darwin-aarch64",
            package_type="binary",
            static_binary_name="macos-aarch64",
        ),
        "binary_ppc64le": BuildConfig(
            name="binary_ppc64le",
            compiler="clang-16-ppc64le",
            package_type="binary",
            static_binary_name="powerpc64le",
        ),
        "binary_amd64_compat": BuildConfig(
            name="binary_amd64_compat",
            compiler="clang-16-amd64-compat",
            package_type="binary",
            static_binary_name="amd64compat",
            comment="SSE2-only build",
        ),
        "binary_riscv64": BuildConfig(
            name="binary_riscv64",
            compiler="clang-16-riscv64",
            package_type="binary",
            static_binary_name="riscv64",
        ),
        "binary_s390x": BuildConfig(
            name="binary_s390x",
            compiler="clang-16-s390x",
            package_type="binary",
            static_binary_name="s390x",
        ),
    },
    builds_report_config={
        "ClickHouse build check": [
            "package_release",
            "package_aarch64",
            "package_asan",
            "package_ubsan",
            "package_tsan",
            "package_msan",
            "package_debug",
            "binary_release",
        ],
        "ClickHouse special build check": [
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
        ],
    },
    test_configs={
        "Install packages (amd64)": TestConfig("package_release"),
        "Install packages (arm64)": TestConfig("package_aarch64"),
        "Stateful tests (asan)": TestConfig("package_asan"),
        "Stateful tests (tsan)": TestConfig("package_tsan"),
        "Stateful tests (msan)": TestConfig("package_msan"),
        "Stateful tests (ubsan)": TestConfig("package_ubsan"),
        "Stateful tests (debug)": TestConfig("package_debug"),
        "Stateful tests (release)": TestConfig("package_release"),
        "Stateful tests (aarch64)": TestConfig("package_aarch64"),
        "Stateful tests (release, DatabaseOrdinary)": TestConfig("package_release"),
        "Stateful tests (release, DatabaseReplicated)": TestConfig("package_release"),
        # Stateful tests for parallel replicas
        "Stateful tests (release, ParallelReplicas)": TestConfig("package_release"),
        "Stateful tests (debug, ParallelReplicas)": TestConfig("package_debug"),
        "Stateful tests (asan, ParallelReplicas)": TestConfig("package_asan"),
        "Stateful tests (msan, ParallelReplicas)": TestConfig("package_msan"),
        "Stateful tests (ubsan, ParallelReplicas)": TestConfig("package_ubsan"),
        "Stateful tests (tsan, ParallelReplicas)": TestConfig("package_tsan"),
        # End stateful tests for parallel replicas
        "Stateless tests (asan)": TestConfig("package_asan"),
        "Stateless tests (tsan)": TestConfig("package_tsan"),
        "Stateless tests (msan)": TestConfig("package_msan"),
        "Stateless tests (ubsan)": TestConfig("package_ubsan"),
        "Stateless tests (debug)": TestConfig("package_debug"),
        "Stateless tests (release)": TestConfig("package_release"),
        "Stateless tests (aarch64)": TestConfig("package_aarch64"),
        "Stateless tests (release, wide parts enabled)": TestConfig("package_release"),
        "Stateless tests (release, analyzer)": TestConfig("package_release"),
        "Stateless tests (release, DatabaseOrdinary)": TestConfig("package_release"),
        "Stateless tests (release, DatabaseReplicated)": TestConfig("package_release"),
        "Stateless tests (release, s3 storage)": TestConfig("package_release"),
        "Stateless tests (debug, s3 storage)": TestConfig("package_debug"),
        "Stateless tests (tsan, s3 storage)": TestConfig("package_tsan"),
        "Stress test (asan)": TestConfig("package_asan"),
        "Stress test (tsan)": TestConfig("package_tsan"),
        "Stress test (ubsan)": TestConfig("package_ubsan"),
        "Stress test (msan)": TestConfig("package_msan"),
        "Stress test (debug)": TestConfig("package_debug"),
        "Upgrade check (asan)": TestConfig("package_asan"),
        "Upgrade check (tsan)": TestConfig("package_tsan"),
        "Upgrade check (msan)": TestConfig("package_msan"),
        "Upgrade check (debug)": TestConfig("package_debug"),
        "Integration tests (asan)": TestConfig("package_asan"),
        "Integration tests (asan, analyzer)": TestConfig("package_asan"),
        "Integration tests (tsan)": TestConfig("package_tsan"),
        "Integration tests (release)": TestConfig("package_release"),
        "Integration tests (msan)": TestConfig("package_msan"),
        "Integration tests flaky check (asan)": TestConfig("package_asan"),
        "Compatibility check (amd64)": TestConfig("package_release"),
        "Compatibility check (aarch64)": TestConfig("package_aarch64"),
        "Unit tests (release)": TestConfig("binary_release"),
        "Unit tests (asan)": TestConfig("package_asan"),
        "Unit tests (msan)": TestConfig("package_msan"),
        "Unit tests (tsan)": TestConfig("package_tsan"),
        "Unit tests (ubsan)": TestConfig("package_ubsan"),
        "AST fuzzer (debug)": TestConfig("package_debug"),
        "AST fuzzer (asan)": TestConfig("package_asan"),
        "AST fuzzer (msan)": TestConfig("package_msan"),
        "AST fuzzer (tsan)": TestConfig("package_tsan"),
        "AST fuzzer (ubsan)": TestConfig("package_ubsan"),
        "Stateless tests flaky check (asan)": TestConfig("package_asan"),
        "ClickHouse Keeper Jepsen": TestConfig("binary_release"),
        "ClickHouse Server Jepsen": TestConfig("binary_release"),
        "Performance Comparison": TestConfig("package_release"),
        "Performance Comparison Aarch64": TestConfig("package_aarch64"),
        "SQLancer (release)": TestConfig("package_release"),
        "SQLancer (debug)": TestConfig("package_debug"),
        "Sqllogic test (release)": TestConfig("package_release"),
        "SQLTest": TestConfig("package_release"),
    },
)
CI_CONFIG.validate()


# checks required by Mergeable Check
REQUIRED_CHECKS = [
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
