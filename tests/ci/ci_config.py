#!/usr/bin/env python3

from typing import Dict, TypeVar

ConfValue = TypeVar("ConfValue", str, bool)
BuildConfig = Dict[str, ConfValue]

CI_CONFIG = {
    "build_config": {
        "package_release": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "amd64",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
        },
        "coverity": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "",
            "package_type": "coverity",
            "tidy": "disable",
            "with_coverage": False,
            "official": False,
        },
        "package_aarch64": {
            "compiler": "clang-15-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "aarch64",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_asan": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "address",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_ubsan": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "undefined",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_tsan": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "thread",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_msan": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "memory",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_debug": {
            "compiler": "clang-15",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_release": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_tidy": {
            "compiler": "clang-15",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "debug-amd64",
            "tidy": "enable",
            "with_coverage": False,
        },
        "binary_darwin": {
            "compiler": "clang-15-darwin",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_aarch64": {
            "compiler": "clang-15-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_aarch64_v80compat": {
            "compiler": "clang-15-aarch64-v80compat",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "aarch64v80compat",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_freebsd": {
            "compiler": "clang-15-freebsd",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "freebsd",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_darwin_aarch64": {
            "compiler": "clang-15-darwin-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos-aarch64",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_ppc64le": {
            "compiler": "clang-15-ppc64le",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "powerpc64le",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_amd64_compat": {
            "compiler": "clang-15-amd64-compat",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "amd64compat",
            "tidy": "disable",
            "with_coverage": False,
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
        "Integration tests (asan)": {
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
        "Compatibility check": {
            "required_build": "package_release",
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
    },
}  # type: dict

# checks required by Mergeable Check
REQUIRED_CHECKS = [
    "Fast test",
    "Style Check",
    "ClickHouse build check",
    "ClickHouse special build check",
    "Stateful tests (release)",
    "Stateless tests (release)",
    "Unit tests (release-clang)",
    "Unit tests (asan)",
    "Unit tests (msan)",
    "Unit tests (tsan)",
    "Unit tests (ubsan)",
]
