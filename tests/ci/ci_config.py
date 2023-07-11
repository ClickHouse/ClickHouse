#!/usr/bin/env python3

from dataclasses import dataclass
from typing import Callable, Dict, TypeVar

ConfValue = TypeVar("ConfValue", str, bool)
BuildConfig = Dict[str, ConfValue]

CI_CONFIG = {
    "build_config": {
        "package_release": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "amd64",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "coverity": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "",
            "package_type": "coverity",
            "tidy": "disable",
            "with_coverage": False,
            "official": False,
            "comment": "A special build for coverity",
        },
        "package_aarch64": {
            "compiler": "clang-16-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "aarch64",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "package_asan": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "address",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "package_ubsan": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "undefined",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "package_tsan": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "thread",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "package_msan": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "memory",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "package_debug": {
            "compiler": "clang-16",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "Note: sparse checkout was used",
        },
        "binary_release": {
            "compiler": "clang-16",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_tidy": {
            "compiler": "clang-16",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "debug-amd64",
            "tidy": "enable",
            "with_coverage": False,
            "comment": "clang-tidy is used for static analysis",
        },
        "binary_darwin": {
            "compiler": "clang-16-darwin",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_aarch64": {
            "compiler": "clang-16-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_aarch64_v80compat": {
            "compiler": "clang-16-aarch64-v80compat",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "aarch64v80compat",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "For ARMv8.1 and older",
        },
        "binary_freebsd": {
            "compiler": "clang-16-freebsd",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "freebsd",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_darwin_aarch64": {
            "compiler": "clang-16-darwin-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos-aarch64",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_ppc64le": {
            "compiler": "clang-16-ppc64le",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "powerpc64le",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
        "binary_amd64_compat": {
            "compiler": "clang-16-amd64-compat",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "amd64compat",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "SSE2-only build",
        },
        "binary_riscv64": {
            "compiler": "clang-16-riscv64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "riscv64",
            "tidy": "disable",
            "with_coverage": False,
            "comment": "",
        },
    },
    "builds_report_config": {
        "ClickHouse build check": [
            "package_release",
            "coverity",
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
            "binary_amd64_compat",
        ],
    },
    "tests_config": {
        # required_build - build name for artifacts
        # force_tests - force success status for tests
        "Install packages (amd64)": {
            "required_build": "package_release",
        },
        "Install packages (arm64)": {
            "required_build": "package_aarch64",
        },
        "Stateful tests (asan)": {
            "required_build": "package_asan",
        },
        "Stateful tests (tsan)": {
            "required_build": "package_tsan",
        },
        "Stateful tests (msan)": {
            "required_build": "package_msan",
        },
        "Stateful tests (ubsan)": {
            "required_build": "package_ubsan",
        },
        "Stateful tests (debug)": {
            "required_build": "package_debug",
        },
        "Stateful tests (release)": {
            "required_build": "package_release",
        },
        "Stateful tests (aarch64)": {
            "required_build": "package_aarch64",
        },
        "Stateful tests (release, DatabaseOrdinary)": {
            "required_build": "package_release",
        },
        "Stateful tests (release, DatabaseReplicated)": {
            "required_build": "package_release",
        },
        # Stateful tests for parallel replicas
        "Stateful tests (release, ParallelReplicas)": {
            "required_build": "package_release",
        },
        "Stateful tests (debug, ParallelReplicas)": {
            "required_build": "package_debug",
        },
        "Stateful tests (asan, ParallelReplicas)": {
            "required_build": "package_asan",
        },
        "Stateful tests (msan, ParallelReplicas)": {
            "required_build": "package_msan",
        },
        "Stateful tests (ubsan, ParallelReplicas)": {
            "required_build": "package_ubsan",
        },
        "Stateful tests (tsan, ParallelReplicas)": {
            "required_build": "package_tsan",
        },
        # End stateful tests for parallel replicas
        "Stateless tests (asan)": {
            "required_build": "package_asan",
        },
        "Stateless tests (tsan)": {
            "required_build": "package_tsan",
        },
        "Stateless tests (msan)": {
            "required_build": "package_msan",
        },
        "Stateless tests (ubsan)": {
            "required_build": "package_ubsan",
        },
        "Stateless tests (debug)": {
            "required_build": "package_debug",
        },
        "Stateless tests (release)": {
            "required_build": "package_release",
        },
        "Stateless tests (aarch64)": {
            "required_build": "package_aarch64",
        },
        "Stateless tests (release, wide parts enabled)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, analyzer)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, DatabaseOrdinary)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, DatabaseReplicated)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, s3 storage)": {
            "required_build": "package_release",
        },
        "Stateless tests (debug, s3 storage)": {
            "required_build": "package_debug",
        },
        "Stateless tests (tsan, s3 storage)": {
            "required_build": "package_tsan",
        },
        "Stress test (asan)": {
            "required_build": "package_asan",
        },
        "Stress test (tsan)": {
            "required_build": "package_tsan",
        },
        "Stress test (ubsan)": {
            "required_build": "package_ubsan",
        },
        "Stress test (msan)": {
            "required_build": "package_msan",
        },
        "Stress test (debug)": {
            "required_build": "package_debug",
        },
        "Upgrade check (asan)": {
            "required_build": "package_asan",
        },
        "Upgrade check (tsan)": {
            "required_build": "package_tsan",
        },
        "Upgrade check (msan)": {
            "required_build": "package_msan",
        },
        "Upgrade check (debug)": {
            "required_build": "package_debug",
        },
        "Integration tests (asan)": {
            "required_build": "package_asan",
        },
        "Integration tests (asan, analyzer)": {
            "required_build": "package_asan",
        },
        "Integration tests (tsan)": {
            "required_build": "package_tsan",
        },
        "Integration tests (release)": {
            "required_build": "package_release",
        },
        "Integration tests (msan)": {
            "required_build": "package_msan",
        },
        "Integration tests flaky check (asan)": {
            "required_build": "package_asan",
        },
        "Compatibility check (amd64)": {
            "required_build": "package_release",
        },
        "Compatibility check (aarch64)": {
            "required_build": "package_aarch64",
        },
        "Unit tests (release-clang)": {
            "required_build": "binary_release",
        },
        "Unit tests (asan)": {
            "required_build": "package_asan",
        },
        "Unit tests (msan)": {
            "required_build": "package_msan",
        },
        "Unit tests (tsan)": {
            "required_build": "package_tsan",
        },
        "Unit tests (ubsan)": {
            "required_build": "package_ubsan",
        },
        "AST fuzzer (debug)": {
            "required_build": "package_debug",
        },
        "AST fuzzer (asan)": {
            "required_build": "package_asan",
        },
        "AST fuzzer (msan)": {
            "required_build": "package_msan",
        },
        "AST fuzzer (tsan)": {
            "required_build": "package_tsan",
        },
        "AST fuzzer (ubsan)": {
            "required_build": "package_ubsan",
        },
        "Stateless tests flaky check (asan)": {
            "required_build": "package_asan",
        },
        "ClickHouse Keeper Jepsen": {
            "required_build": "binary_release",
        },
        "ClickHouse Server Jepsen": {
            "required_build": "binary_release",
        },
        "Performance Comparison": {
            "required_build": "package_release",
            "test_grep_exclude_filter": "",
        },
        "Performance Comparison Aarch64": {
            "required_build": "package_aarch64",
            "test_grep_exclude_filter": "",
        },
        "SQLancer (release)": {
            "required_build": "package_release",
        },
        "SQLancer (debug)": {
            "required_build": "package_debug",
        },
        "Sqllogic test (release)": {
            "required_build": "package_release",
        },
    },
}  # type: dict

# checks required by Mergeable Check
REQUIRED_CHECKS = [
    "ClickHouse build check",
    "ClickHouse special build check",
    "Docs Check",
    "Fast test",
    "Stateful tests (release)",
    "Stateless tests (release)",
    "Stateless tests (debug) [1/5]",
    "Stateless tests (debug) [2/5]",
    "Stateless tests (debug) [3/5]",
    "Stateless tests (debug) [4/5]",
    "Stateless tests (debug) [5/5]",
    "AST fuzzer (asan)",
    "AST fuzzer (msan)",
    "AST fuzzer (tsan)",
    "AST fuzzer (ubsan)",
    "AST fuzzer (debug)",
    "Compatibility check (aarch64)",
    "Compatibility check (amd64)",
    "Install packages (amd64)",
    "Install packages (arm64)",
    "Integration tests (asan) [1/6]",
    "Integration tests (asan) [2/6]",
    "Integration tests (asan) [3/6]",
    "Integration tests (asan) [4/6]",
    "Integration tests (asan) [5/6]",
    "Integration tests (asan) [6/6]",
    "Integration tests (release) [1/4]",
    "Integration tests (release) [2/4]",
    "Integration tests (release) [3/4]",
    "Integration tests (release) [4/4]",
    "Integration tests (tsan) [1/6]",
    "Integration tests (tsan) [2/6]",
    "Integration tests (tsan) [3/6]",
    "Integration tests (tsan) [4/6]",
    "Integration tests (tsan) [5/6]",
    "Integration tests (tsan) [6/6]",
    "Integration tests flaky check (asan)",
    "Stateful tests (aarch64)",
    "Stateful tests (asan)",
    "Stateful tests (asan, ParallelReplicas)",
    "Stateful tests (debug)",
    "Stateful tests (debug, ParallelReplicas)",
    "Stateful tests (msan)",
    "Stateful tests (msan, ParallelReplicas)",
    "Stateful tests (release, ParallelReplicas)",
    "Stateful tests (tsan)",
    "Stateful tests (tsan, ParallelReplicas)",
    "Stateful tests (ubsan)",
    "Stateful tests (ubsan, ParallelReplicas)",
    "Stateless tests (aarch64)",
    "Stateless tests (asan) [1/4]",
    "Stateless tests (asan) [2/4]",
    "Stateless tests (asan) [3/4]",
    "Stateless tests (asan) [4/4]",
    "Stateless tests (debug) [1/5]",
    "Stateless tests (debug) [2/5]",
    "Stateless tests (debug) [3/5]",
    "Stateless tests (debug) [4/5]",
    "Stateless tests (debug) [5/5]",
    "Stateless tests (debug, s3 storage) [1/6]",
    "Stateless tests (debug, s3 storage) [2/6]",
    "Stateless tests (debug, s3 storage) [3/6]",
    "Stateless tests (debug, s3 storage) [4/6]",
    "Stateless tests (debug, s3 storage) [5/6]",
    "Stateless tests (debug, s3 storage) [6/6]",
    "Stateless tests (msan) [1/6]",
    "Stateless tests (msan) [2/6]",
    "Stateless tests (msan) [3/6]",
    "Stateless tests (msan) [4/6]",
    "Stateless tests (msan) [5/6]",
    "Stateless tests (msan) [6/6]",
    "Stateless tests (release, DatabaseReplicated) [1/4]",
    "Stateless tests (release, DatabaseReplicated) [2/4]",
    "Stateless tests (release, DatabaseReplicated) [3/4]",
    "Stateless tests (release, DatabaseReplicated) [4/4]",
    "Stateless tests (release, s3 storage) [1/2]",
    "Stateless tests (release, s3 storage) [2/2]",
    "Stateless tests (release, wide parts enabled)",
    "Stateless tests (tsan) [1/5]",
    "Stateless tests (tsan) [2/5]",
    "Stateless tests (tsan) [3/5]",
    "Stateless tests (tsan) [4/5]",
    "Stateless tests (tsan) [5/5]",
    "Stateless tests (tsan, s3 storage) [1/5]",
    "Stateless tests (tsan, s3 storage) [2/5]",
    "Stateless tests (tsan, s3 storage) [3/5]",
    "Stateless tests (tsan, s3 storage) [4/5]",
    "Stateless tests (tsan, s3 storage) [5/5]",
    "Stateless tests (ubsan) [1/2]",
    "Stateless tests (ubsan) [2/2]",
    "Stress test (asan)",
    "Stress test (debug)",
    "Stress test (msan)",
    "Stress test (tsan)",
    "Stress test (ubsan)",
    "Upgrade check (asan)",
    "Upgrade check (debug)",
    "Upgrade check (msan)",
    "Upgrade check (tsan)",
    "Style Check",
    "Unit tests (asan)",
    "Unit tests (msan)",
    "Unit tests (release-clang)",
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
