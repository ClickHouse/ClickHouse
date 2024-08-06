#!/usr/bin/env python3

# type: ignore

import unittest
from ci import CiOptions
from ci_config import JobNames

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
            ci_options.include_keywords, ["integration", "foo_bar", "stateless"]
        )
        self.assertCountEqual(
            ci_options.exclude_keywords,
            ["tsan", "aarch64", "analyzer", "s3_storage", "coverage"],
        )
        jobs_to_do = list(JobNames)
        jobs_to_skip = []
        job_params = {}
        jobs_to_do, jobs_to_skip, job_params = ci_options.apply(
            jobs_to_do, jobs_to_skip, job_params
        )
        self.assertCountEqual(
            jobs_to_do,
            [
                "Style check",
                "package_release",
                "package_asan",
                "package_ubsan",
                "package_debug",
                "package_msan",
                "Stateless tests (asan)",
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
        jobs_to_do = list(JobNames)
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
