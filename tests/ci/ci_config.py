#!/usr/bin/env python3

CI_CONFIG = {
    "build_config": {
        "package_release": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "alien_pkgs": True,
            "tidy": "disable",
            "with_coverage": False
        },
        "performance": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "performance",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_gcc": {
            "compiler": "gcc-10",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "package_asan": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "address",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "package_ubsan": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "undefined",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "package_tsan": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "thread",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "package_msan": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "memory",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "package_debug": {
            "compiler": "clang-11",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_release": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_tidy": {
            "compiler": "clang-11",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "enable",
            "with_coverage": False
        },
        "binary_splitted": {
            "compiler": "clang-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "splitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_darwin": {
            "compiler": "clang-11-darwin",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_aarch64": {
            "compiler": "clang-11-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_freebsd": {
            "compiler": "clang-11-freebsd",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_darwin_aarch64": {
            "compiler": "clang-11-darwin-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        "binary_ppc64le": {
            "compiler": "clang-11-ppc64le",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        }
    },
    "builds_report_config": {
        "ClickHouse build check (actions)": [
            "package_release",
            "performance",
            "package_asan",
            "package_ubsan",
            "package_tsan",
            "package_msan",
            "package_debug",
            "binary_release"
        ],
        "ClickHouse special build check (actions)": [
            "binary_tidy",
            "binary_splitted",
            "binary_darwin",
            "binary_arrach64",
            "binary_freebsd",
            "binary_darwin_aarch64"
        ],
    },
    "tests_config": {
        "Stateful tests (address, actions)": {
            "required_build": "package_asan",
        },
        "Stateful tests (thread, actions)": {
            "required_build": "package_tsan",
        },
        "Stateful tests (memory, actions)": {
            "required_build": "package_msan",
        },
        "Stateful tests (ubsan, actions)": {
            "required_build": "package_ubsan",
        },
        "Stateful tests (debug, actions)": {
            "required_build": "package_debug",
        },
        "Stateful tests (release, actions)": {
            "required_build": "package_release",
        },
        "Stateful tests (release, DatabaseOrdinary, actions)": {
            "required_build": "package_release",
        },
        "Stateful tests (release, DatabaseReplicated, actions)": {
            "required_build": "package_release",
        },
        "Stateless tests (address, actions)": {
            "required_build": "package_asan",
        },
        "Stateless tests (thread, actions)": {
            "required_build": "package_tsan",
        },
        "Stateless tests (memory, actions)": {
            "required_build": "package_msan",
        },
        "Stateless tests (ubsan, actions)": {
            "required_build": "package_ubsan",
        },
        "Stateless tests (debug, actions)": {
            "required_build": "package_debug",
        },
        "Stateless tests (release, actions)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, wide parts enabled, actions)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, DatabaseOrdinary, actions)": {
            "required_build": "package_release",
        },
        "Stateless tests (release, DatabaseReplicated, actions)": {
            "required_build": "package_release",
        },
        "Stress test (address, actions)": {
            "required_build": "package_asan",
        },
        "Stress test (thread, actions)": {
            "required_build": "package_tsan",
        },
        "Stress test (undefined, actions)": {
            "required_build": "package_ubsan",
        },
        "Stress test (memory, actions)": {
            "required_build": "package_msan",
        },
        "Stress test (debug, actions)": {
            "required_build": "package_debug",
        },
        "Integration tests (asan, actions)": {
            "required_build": "package_asan",
        },
        "Integration tests (thread, actions)": {
            "required_build": "package_tsan",
        },
        "Integration tests (release, actions)": {
            "required_build": "package_release",
        },
        "Integration tests (memory, actions)": {
            "required_build": "package_msan",
        },
        "Integration tests flaky check (asan, actions)": {
            "required_build": "package_asan",
        },
        "Compatibility check (actions)": {
            "required_build": "package_release",
        },
        "Split build smoke test (actions)": {
            "required_build": "binary_splitted",
        },
        "Testflows check (actions)": {
            "required_build": "package_release",
        },
        "Unit tests (release-gcc, actions)": {
            "required_build": "binary_gcc",
        },
        "Unit tests (release-clang, actions)": {
            "required_build": "binary_release",
        },
        "Unit tests (asan, actions)": {
            "required_build": "package_asan",
        },
        "Unit tests (msan, actions)": {
            "required_build": "package_msan",
        },
        "Unit tests (tsan, actions)": {
            "required_build": "package_tsan",
        },
        "Unit tests (ubsan, actions)": {
            "required_build": "package_ubsan",
        },
        "AST fuzzer (debug, actions)": {
            "required_build": "package_debug",
        },
        "AST fuzzer (ASan, actions)": {
            "required_build": "package_asan",
        },
        "AST fuzzer (MSan, actions)": {
            "required_build": "package_msan",
        },
        "AST fuzzer (TSan, actions)": {
            "required_build": "package_tsan",
        },
        "AST fuzzer (UBSan, actions)": {
            "required_build": "package_ubsan",
        },
        "Release (actions)": {
            "required_build": "package_release",
        },
        "Stateless tests flaky check (address, actions)": {
            "required_build": "package_asan",
        },
        "ClickHouse Keeper Jepsen (actions)": {
            "required_build": "binary_release",
        }
    }
}
