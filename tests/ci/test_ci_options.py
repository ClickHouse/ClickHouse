#!/usr/bin/env python3

# type: ignore

import unittest

from ci_config import CI
from ci_settings import CiSettings

_TEST_BODY_1 = """
#### Run only:
- [ ] <!---ci_set_required--> Some Set
- [x] <!---ci_set_arm--> Integration tests (arm64)
- [x] <!---ci_include_foo--> Integration tests
- [x] <!---ci_include_foo_Bar--> Integration tests
- [ ] <!---ci_include_bar--> Integration tests
- [x] <!---ci_exclude_foo--> some invalid mask - should be skipped
- [x] <!---ci_exclude_Foo_bar--> Integration tests
- [ ] <!---ci_exclude_bar--> Integration tests

#### CI options:
- [ ] <!---do_not_test--> do not test (only style check)
- [x] <!---woolen_wolfdog--> Woolen Wolfdog CI
- [x] <!---no_merge_commit--> disable merge-commit (no merge from master before tests)
- [ ] <!---no_ci_cache--> disable CI cache (job reuse)

#### Only specified batches in multi-batch jobs:
- [x] <!---batch_0--> 1
- [ ] <!---batch_1--> 2
"""

_TEST_BODY_2 = """
- [x] <!---ci_include_integration--> MUST include integration tests
- [x] <!---ci_include_stateless--> MUST include stateless tests
- [x] <!---ci_include_azure--> MUST include azure
- [x] <!---ci_include_foo_Bar--> no action must be applied
- [ ] <!---ci_include_bar--> no action must be applied
- [x] <!---ci_exclude_tsan|foobar--> MUST exclude tsan
- [x] <!---ci_exclude_aarch64--> MUST exclude aarch64
- [x] <!---ci_exclude_analyzer--> MUST exclude test with analazer
- [ ] <!---ci_exclude_bar--> no action applied
- [x] <!---ci_exclude_s3_storage--> Must exclude statless test with s3 storage
- [x] <!---ci_exclude_coverage--> Must exclude tests on coverage build
"""

_TEST_BODY_3 = """
- [x] <!---ci_include_analyzer--> Must include all tests for analyzer
"""

_TEST_BODY_4 = """
"""


_TEST_JOB_LIST = [
    "Style check",
    "Fast test",
    "package_release",
    "package_asan",
    "package_aarch64",
    "package_release_coverage",
    "package_debug",
    "package_tsan",
    "package_msan",
    "package_ubsan",
    "binary_release",
    "fuzzers",
    "Docker server image",
    "Docker keeper image",
    "Install packages (release)",
    "Install packages (aarch64)",
    "Stateless tests (debug)",
    "Stateless tests (release)",
    "Stateless tests (coverage)",
    "Stateless tests (aarch64)",
    "Stateless tests (asan)",
    "Stateless tests (tsan)",
    "Stateless tests (msan)",
    "Stateless tests (ubsan)",
    "Stateless tests (release, old analyzer, s3, DatabaseReplicated)",
    "Stateless tests (debug, s3 storage)",
    "Stateless tests (tsan, s3 storage)",
    "Stateless tests flaky check (asan)",
    "Stateless tests (azure, asan)",
    "Stateful tests (debug)",
    "Stateful tests (release)",
    "Stateful tests (coverage)",
    "Stateful tests (aarch64)",
    "Stateful tests (asan)",
    "Stateful tests (tsan)",
    "Stateful tests (msan)",
    "Stateful tests (ubsan)",
    "Stateful tests (release, ParallelReplicas)",
    "Stateful tests (debug, ParallelReplicas)",
    "Stateful tests (asan, ParallelReplicas)",
    "Stateful tests (msan, ParallelReplicas)",
    "Stateful tests (ubsan, ParallelReplicas)",
    "Stateful tests (tsan, ParallelReplicas)",
    "Stress test (asan)",
    "Stress test (tsan)",
    "Stress test (ubsan)",
    "Stress test (msan)",
    "Stress test (debug)",
    "Integration tests (release)",
    "Integration tests (asan)",
    "Integration tests (asan, old analyzer)",
    "Integration tests (tsan)",
    "Integration tests (aarch64)",
    "Integration tests flaky check (asan)",
    "Upgrade check (debug)",
    "Upgrade check (asan)",
    "Upgrade check (tsan)",
    "Upgrade check (msan)",
    "Unit tests (release)",
    "Unit tests (asan)",
    "Unit tests (msan)",
    "Unit tests (tsan)",
    "Unit tests (ubsan)",
    "AST fuzzer (debug)",
    "AST fuzzer (asan)",
    "AST fuzzer (msan)",
    "AST fuzzer (tsan)",
    "AST fuzzer (ubsan)",
    "ClickHouse Keeper Jepsen",
    "ClickHouse Server Jepsen",
    "Performance Comparison (release)",
    "Performance Comparison (aarch64)",
    "Sqllogic test (release)",
    "SQLancer (release)",
    "SQLancer (debug)",
    "SQLTest",
    "Compatibility check (release)",
    "Compatibility check (aarch64)",
    "ClickBench (release)",
    "ClickBench (aarch64)",
    "libFuzzer tests",
    "Builds",
    "Docs check",
    "Bugfix validation",
]

_TEST_JOB_LIST_2 = ["Style check", "Fast test", "fuzzers"]


class TestCIOptions(unittest.TestCase):
    def test_pr_body_parsing(self):
        ci_options = CiSettings.create_from_pr_message(
            _TEST_BODY_1, update_from_api=False
        )
        self.assertFalse(ci_options.do_not_test)
        self.assertFalse(ci_options.no_ci_cache)
        self.assertTrue(ci_options.no_merge_commit)
        self.assertTrue(ci_options.woolen_wolfdog)
        self.assertEqual(ci_options.ci_sets, ["ci_set_arm"])
        self.assertCountEqual(ci_options.include_keywords, ["foo", "foo_bar"])
        self.assertCountEqual(ci_options.exclude_keywords, ["foo", "foo_bar"])

    def test_options_applied(self):
        self.maxDiff = None
        ci_options = CiSettings.create_from_pr_message(
            _TEST_BODY_2, update_from_api=False
        )
        self.assertFalse(ci_options.woolen_wolfdog)
        self.assertCountEqual(
            ci_options.include_keywords,
            ["integration", "foo_bar", "stateless", "azure"],
        )
        self.assertCountEqual(
            ci_options.exclude_keywords,
            ["tsan", "foobar", "aarch64", "analyzer", "s3_storage", "coverage"],
        )

        jobs_configs = {
            job: CI.JobConfig(runner_type=CI.Runners.STYLE_CHECKER)
            for job in _TEST_JOB_LIST
        }
        # check "fuzzers" appears in the result due to the label
        jobs_configs["fuzzers"].run_by_labels = ["TEST_LABEL"]
        jobs_configs["Integration tests (asan)"].release_only = (
            True  # still must be included as it's set with include keywords
        )
        filtered_jobs = list(
            ci_options.apply(
                jobs_configs,
                is_release=False,
                is_pr=True,
                is_mq=False,
                labels=["TEST_LABEL"],
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Style check",
                "fuzzers",
                "package_release",
                "package_asan",
                "package_debug",
                "package_msan",
                "package_ubsan",
                "package_aarch64",
                "package_release_coverage",
                "package_tsan",
                "binary_release",
                "Stateless tests (asan)",
                "Stateless tests (azure, asan)",
                "Stateless tests flaky check (asan)",
                "Stateless tests (msan)",
                "Stateless tests (ubsan)",
                "Stateless tests (debug)",
                "Stateless tests (release)",
                "Integration tests (release)",
                "Integration tests (asan)",
                "Integration tests flaky check (asan)",
            ],
        )

    def test_options_applied_2(self):
        jobs_configs = {
            job: CI.JobConfig(runner_type=CI.Runners.STYLE_CHECKER)
            for job in _TEST_JOB_LIST_2
        }
        jobs_configs["Style check"].release_only = True
        jobs_configs["Fast test"].pr_only = True
        jobs_configs["fuzzers"].run_by_labels = ["TEST_LABEL"]
        # no settings are set
        filtered_jobs = list(
            CiSettings().apply(
                jobs_configs, is_release=False, is_pr=False, is_mq=True, labels=[]
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Fast test",
            ],
        )
        filtered_jobs = list(
            CiSettings().apply(
                jobs_configs, is_release=False, is_pr=True, is_mq=False, labels=[]
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Fast test",
            ],
        )
        filtered_jobs = list(
            CiSettings().apply(
                jobs_configs, is_release=True, is_pr=False, is_mq=False, labels=[]
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Style check",
            ],
        )

    def test_options_applied_3(self):
        ci_settings = CiSettings()
        ci_settings.include_keywords = ["Style"]
        jobs_configs = {
            job: CI.JobConfig(runner_type=CI.Runners.STYLE_CHECKER)
            for job in _TEST_JOB_LIST_2
        }
        jobs_configs["Style check"].release_only = True
        jobs_configs["Fast test"].pr_only = True
        # no settings are set
        filtered_jobs = list(
            ci_settings.apply(
                jobs_configs,
                is_release=False,
                is_pr=True,
                is_mq=False,
                labels=["TEST_LABEL"],
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Style check",
                "fuzzers",
            ],
        )

        ci_settings.include_keywords = ["Fast"]
        filtered_jobs = list(
            ci_settings.apply(
                jobs_configs,
                is_release=True,
                is_pr=False,
                is_mq=False,
                labels=["TEST_LABEL"],
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            ["Style check", "fuzzers"],
        )

    def test_options_applied_4(self):
        self.maxDiff = None
        ci_options = CiSettings.create_from_pr_message(
            _TEST_BODY_3, update_from_api=False
        )
        self.assertCountEqual(ci_options.include_keywords, ["analyzer"])
        self.assertIsNone(ci_options.exclude_keywords)
        jobs_configs = {
            job: CI.JobConfig(runner_type=CI.Runners.STYLE_CHECKER)
            for job in _TEST_JOB_LIST
        }
        # check "fuzzers" does not appears in the result
        jobs_configs["fuzzers"].run_by_labels = ["TEST_LABEL"]
        jobs_configs["Integration tests (asan)"].release_only = True
        filtered_jobs = list(
            ci_options.apply(
                jobs_configs,
                is_release=False,
                is_pr=True,
                is_mq=False,
                labels=["TEST_LABEL"],
            )
        )
        self.assertCountEqual(
            filtered_jobs,
            [
                "Style check",
                "Integration tests (asan, old analyzer)",
                "package_release",
                "Stateless tests (release, old analyzer, s3, DatabaseReplicated)",
                "package_asan",
                "fuzzers",
                "package_aarch64",
                "package_release_coverage",
                "package_debug",
                "package_tsan",
                "package_msan",
                "package_ubsan",
                "binary_release",
            ],
        )
