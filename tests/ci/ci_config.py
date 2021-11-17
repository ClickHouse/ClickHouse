#!/usr/bin/env python3

CI_CONFIG = {
    "build_config": [
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "alien_pkgs": True,
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "",
            "package_type": "performance",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "gcc-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "address",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "undefined",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "thread",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "memory",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "gcc-11",
            "build_type": "",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "unbundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        }
    ],
    "special_build_config": [
        {
            "compiler": "clang-13",
            "build_type": "debug",
            "sanitizer": "",
            "package_type": "deb",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "enable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "splitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13-darwin",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13-freebsd",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13-darwin-aarch64",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        },
        {
            "compiler": "clang-13-ppc64le",
            "build_type": "",
            "sanitizer": "",
            "package_type": "binary",
            "bundled": "bundled",
            "splitted": "unsplitted",
            "tidy": "disable",
            "with_coverage": False
        }
    ],
    "tests_config": {
        "Stateful tests (address, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (thread, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (memory, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (ubsan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "undefined",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (debug, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "debug",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (release, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (release, DatabaseOrdinary, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateful tests (release, DatabaseReplicated, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (address, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (thread, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (memory, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (ubsan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "undefined",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (debug, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "debug",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (release, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (unbundled, actions)": {
            "required_build_properties": {
                "compiler": "gcc-11",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "unbundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (release, wide parts enabled, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (release, DatabaseOrdinary, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests (release, DatabaseReplicated, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stress test (address, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stress test (thread, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stress test (undefined, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "undefined",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stress test (memory, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stress test (debug, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "debug",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Integration tests (asan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Integration tests (thread, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Integration tests (release, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Integration tests (memory, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Integration tests flaky check (asan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Compatibility check (actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Split build smoke test (actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "binary",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "splitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Testflows check (actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (release-gcc, actions)": {
            "required_build_properties": {
                "compiler": "gcc-11",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (release-clang, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "binary",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (asan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (msan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (tsan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Unit tests (ubsan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "AST fuzzer (debug, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "debug",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "AST fuzzer (ASan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "AST fuzzer (MSan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "memory",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "AST fuzzer (TSan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "thread",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "AST fuzzer (UBSan, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "undefined",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Release (actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "Stateless tests flaky check (address, actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "deb",
                "build_type": "relwithdebuginfo",
                "sanitizer": "address",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        },
        "ClickHouse Keeper Jepsen (actions)": {
            "required_build_properties": {
                "compiler": "clang-13",
                "package_type": "binary",
                "build_type": "relwithdebuginfo",
                "sanitizer": "none",
                "bundled": "bundled",
                "splitted": "unsplitted",
                "clang_tidy": "disable",
                "with_coverage": False
            }
        }
    }
}

def build_config_to_string(build_config):
    if build_config["package_type"] == "performance":
        return "performance"

    return "_".join([
        build_config['compiler'],
        build_config['build_type'] if build_config['build_type'] else "relwithdebuginfo",
        build_config['sanitizer'] if build_config['sanitizer'] else "none",
        build_config['bundled'],
        build_config['splitted'],
        'tidy' if 'tidy' in build_config and build_config['tidy'] == 'enable' else 'notidy',
        'with_coverage' if 'with_coverage' in build_config and build_config['with_coverage'] else 'without_coverage',
        build_config['package_type'],
    ])
