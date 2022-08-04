#!/usr/bin/env python3

from typing import Dict, TypeVar

ConfValue = TypeVar("ConfValue", str, bool)
BuildConfig = Dict[str, ConfValue]

CI_CONFIG = {
    "build_config": {
        "package_release": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "amd64",
            "bundled": "bundled",
            "libraries": "static",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
        },
        "coverity": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "",
            "package_type": "coverity",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
            "official": False,
        },
        "package_aarch64": {
            "compiler": "clang-14-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "static_binary_name": "aarch64",
            "bundled": "bundled",
            "libraries": "static",
            "additional_pkgs": True,
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_asan": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "address",
            "package_type": "deb",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_ubsan": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "undefined",
            "package_type": "deb",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_tsan": {
            "compiler": "clang-15",
            "build_type": "",
            "sanitizer": "thread",
            "package_type": "deb",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_msan": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "memory",
            "package_type": "deb",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "package_debug": {
            "compiler": "clang-14",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_release": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_tidy": {
            "compiler": "clang-14",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "debug-amd64",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "enable",
            "with_coverage": False,
        },
        "binary_shared": {
            "compiler": "clang-14",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "libraries": "shared",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_darwin": {
            "compiler": "clang-14-darwin",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_aarch64": {
            "compiler": "clang-14-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_freebsd": {
            "compiler": "clang-14-freebsd",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "freebsd",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_darwin_aarch64": {
            "compiler": "clang-14-darwin-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "macos-aarch64",
            "bundled": "bundled",
            "libraries": "static",
            "tidy": "disable",
            "with_coverage": False,
        },
        "binary_ppc64le": {
            "compiler": "clang-14-ppc64le",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "static_binary_name": "powerpc64le",
            "bundled": "bundled",
            "libraries": "static",
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
            "binary_shared",
            "binary_darwin",
            "binary_aarch64",
            "binary_freebsd",
            "binary_darwin_aarch64",
            "binary_ppc64le",
        ],
    },
    "tests_config": {
        # required_build - build name for artifacts
        # force_tests - force success status for tests
        "Stateful tests (address)": {
            "required_build": "package_asan",
        },
        "Stateful tests (thread)": {
            "required_build": "package_tsan",
        },
        "Stateful tests (memory)": {
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
        "Stateless tests (address)": {
            "required_build": "package_asan",
        },
        "Stateless tests (thread)": {
            "required_build": "package_tsan",
        },
        "Stateless tests (memory)": {
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
        "Stress test (address)": {
            "required_build": "package_asan",
        },
        "Stress test (thread)": {
            "required_build": "package_tsan",
        },
        "Stress test (undefined)": {
            "required_build": "package_ubsan",
        },
        "Stress test (memory)": {
            "required_build": "package_msan",
        },
        "Stress test (debug)": {
            "required_build": "package_debug",
        },
        "Integration tests (asan)": {
            "required_build": "package_asan",
        },
        "Integration tests (thread)": {
            "required_build": "package_tsan",
        },
        "Integration tests (release)": {
            "required_build": "package_release",
        },
        "Integration tests (memory)": {
            "required_build": "package_msan",
        },
        "Integration tests flaky check (asan)": {
            "required_build": "package_asan",
        },
        "Compatibility check": {
            "required_build": "package_release",
        },
        "Split build smoke test": {
            "required_build": "binary_shared",
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
        "AST fuzzer (ASan)": {
            "required_build": "package_asan",
        },
        "AST fuzzer (MSan)": {
            "required_build": "package_msan",
        },
        "AST fuzzer (TSan)": {
            "required_build": "package_tsan",
        },
        "AST fuzzer (UBSan)": {
            "required_build": "package_ubsan",
        },
        "Stateless tests flaky check (address)": {
            "required_build": "package_asan",
        },
        "ClickHouse Keeper Jepsen": {
            "required_build": "binary_release",
        },
        "Performance Comparison": {
            "required_build": "package_release",
            "test_grep_exclude_filter": "",
        },
        "Performance Comparison Aarch64": {
            "required_build": "package_aarch64",
            "test_grep_exclude_filter": "constant_column_search",
        },
    },
}  # type: dict
