#!/usr/bin/env python3

# type: ignore

import unittest
from ci import CiOptions

_TEST_BODY_1 = """
#### Run only:
- [x] <!---ci_set_integration--> Integration tests
- [ ] <!---ci_set_arm--> Integration tests (arm64)
- [x] <!---ci_include_foo--> Integration tests
- [x] <!---ci_include_foo_Bar--> Integration tests
- [ ] <!---ci_include_bar--> Integration tests
- [x] <!---ci_exclude_foo--> some invalid mask - should be skipped
- [x] <!---ci_exclude_Foo_bar--> Integration tests
- [ ] <!---ci_exclude_bar--> Integration tests

#### CI options:
- [ ] <!---do_not_test--> do not test (only style check)
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
- [x] <!---ci_exclude_tsan--> MUST exclude tsan
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
    "Docker server image",
    "Docker keeper image",
    "Install packages (amd64)",
    "Install packages (arm64)",
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
    "Performance Comparison",
    "Performance Comparison Aarch64",
    "Sqllogic test (release)",
    "SQLancer (release)",
    "SQLancer (debug)",
    "SQLTest",
    "Compatibility check (amd64)",
    "Compatibility check (aarch64)",
    "ClickBench (amd64)",
    "ClickBench (aarch64)",
    "libFuzzer tests",
    "ClickHouse build check",
    "ClickHouse special build check",
    "Docs check",
    "Bugfix validation",
]


class TestCIOptions(unittest.TestCase):
    def test_pr_body_parsing(self):
        ci_options = CiOptions.create_from_pr_message(
            _TEST_BODY_1, update_from_api=False
        )
        self.assertFalse(ci_options.do_not_test)
        self.assertFalse(ci_options.no_ci_cache)
        self.assertTrue(ci_options.no_merge_commit)
        self.assertEqual(ci_options.ci_sets, ["ci_set_integration"])
        self.assertCountEqual(ci_options.include_keywords, ["foo", "foo_bar"])
        self.assertCountEqual(ci_options.exclude_keywords, ["foo", "foo_bar"])

    def test_options_applied(self):
        self.maxDiff = None
        ci_options = CiOptions.create_from_pr_message(
            _TEST_BODY_2, update_from_api=False
        )
        self.assertCountEqual(
            ci_options.include_keywords,
            ["integration", "foo_bar", "stateless", "azure"],
        )
        self.assertCountEqual(
            ci_options.exclude_keywords,
            ["tsan", "aarch64", "analyzer", "s3_storage", "coverage"],
        )
        jobs_to_do = list(_TEST_JOB_LIST)
        jobs_to_skip = []
        job_params = {
            "Stateless tests (azure, asan)": {
                "batches": list(range(3)),
                "num_batches": 3,
                "run_if_ci_option_include_set": True,
            }
        }
        jobs_to_do, jobs_to_skip, job_params = ci_options.apply(
            jobs_to_do, jobs_to_skip, job_params
        )
        self.assertCountEqual(
            jobs_to_do,
            [
                "Style check",
                "package_release",
                "package_asan",
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
        self.maxDiff = None
        ci_options = CiOptions.create_from_pr_message(
            _TEST_BODY_3, update_from_api=False
        )
        self.assertCountEqual(ci_options.include_keywords, ["analyzer"])
        self.assertIsNone(ci_options.exclude_keywords)
        jobs_to_do = list(_TEST_JOB_LIST)
        jobs_to_skip = []
        job_params = {}
        jobs_to_do, jobs_to_skip, job_params = ci_options.apply(
            jobs_to_do, jobs_to_skip, job_params
        )
        self.assertCountEqual(
            jobs_to_do,
            [
                "Style check",
                "Integration tests (asan, old analyzer)",
                "package_release",
                "Stateless tests (release, old analyzer, s3, DatabaseReplicated)",
                "package_asan",
            ],
        )

    def test_options_applied_3(self):
        self.maxDiff = None
        ci_options = CiOptions.create_from_pr_message(
            _TEST_BODY_4, update_from_api=False
        )
        self.assertIsNone(ci_options.include_keywords, None)
        self.assertIsNone(ci_options.exclude_keywords, None)
        jobs_to_do = list(_TEST_JOB_LIST)
        jobs_to_skip = []
        job_params = {}

        for job in _TEST_JOB_LIST:
            if "Stateless" in job:
                job_params[job] = {
                    "batches": list(range(3)),
                    "num_batches": 3,
                    "run_if_ci_option_include_set": "azure" in job,
                }
            else:
                job_params[job] = {"run_if_ci_option_include_set": False}

        jobs_to_do, jobs_to_skip, job_params = ci_options.apply(
            jobs_to_do, jobs_to_skip, job_params
        )
        self.assertNotIn(
            "Stateless tests (azure, asan)",
            jobs_to_do,
        )
