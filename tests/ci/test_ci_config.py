#!/usr/bin/env python3
import copy
import random
import unittest

import ci as CIPY
from ci_cache import CiCache
from ci_config import CI
from ci_settings import CiSettings
from ci_utils import Utils
from pr_info import EventType, PRInfo
from s3_helper import S3Helper

_TEST_EVENT_JSON = {"dummy": "dummy"}

# pylint:disable=protected-access,union-attr


class TestCIConfig(unittest.TestCase):
    def test_runner_config(self):
        """check runner is provided w/o exception"""
        for job in CI.JobNames:
            self.assertIn(CI.JOB_CONFIGS[job].runner_type, CI.Runners)
            if (
                job
                in (
                    CI.JobNames.STYLE_CHECK,
                    CI.JobNames.BUILD_CHECK,
                )
                or "jepsen" in job.lower()
            ):
                self.assertTrue(
                    "style" in CI.JOB_CONFIGS[job].runner_type,
                    f"Job [{job}] must have style-checker(-aarch64) runner",
                )
            elif "binary_" in job.lower() or "package_" in job.lower():
                if job.lower() in (
                    CI.BuildNames.PACKAGE_AARCH64,
                    CI.BuildNames.PACKAGE_ARM_ASAN,
                ):
                    self.assertTrue(
                        CI.JOB_CONFIGS[job].runner_type in (CI.Runners.BUILDER_ARM,),
                        f"Job [{job}] must have [{CI.Runners.BUILDER_ARM}] runner",
                    )
                else:
                    self.assertTrue(
                        CI.JOB_CONFIGS[job].runner_type in (CI.Runners.BUILDER,),
                        f"Job [{job}] must have [{CI.Runners.BUILDER}] runner",
                    )
            elif "aarch64" in job.lower():
                self.assertTrue(
                    "aarch" in CI.JOB_CONFIGS[job].runner_type,
                    f"Job [{job}] does not match runner [{CI.JOB_CONFIGS[job].runner_type}]",
                )
            else:
                self.assertTrue(
                    "aarch" not in CI.JOB_CONFIGS[job].runner_type,
                    f"Job [{job}] does not match runner [{CI.JOB_CONFIGS[job].runner_type}]",
                )

    def test_common_configs_applied_properly(self):
        for job in CI.JobNames:
            if CI.JOB_CONFIGS[job].job_name_keyword:
                self.assertTrue(
                    CI.JOB_CONFIGS[job].job_name_keyword.lower()
                    in Utils.normalize_string(job),
                    f"Job [{job}] apparently uses wrong common config with job keyword [{CI.JOB_CONFIGS[job].job_name_keyword}]",
                )

    def test_job_config_has_proper_values(self):
        for job in CI.JobNames:
            if CI.JOB_CONFIGS[job].reference_job_name:
                reference_job_config = CI.JOB_CONFIGS[
                    CI.JOB_CONFIGS[job].reference_job_name
                ]
                # reference job must run in all workflows and has digest
                self.assertTrue(reference_job_config.pr_only == False)
                self.assertTrue(reference_job_config.release_only == False)
                self.assertTrue(reference_job_config.run_always == False)
                self.assertTrue(reference_job_config.digest != CI.DigestConfig())

    def test_required_checks(self):
        for job in CI.REQUIRED_CHECKS:
            if job in (CI.StatusNames.PR_CHECK, CI.StatusNames.SYNC):
                continue
            self.assertTrue(job in CI.JOB_CONFIGS, f"Job [{job}] not in job config")

    def test_builds_configs(self):
        """build name in the build config must match the job name"""
        for job in CI.JobNames:
            self.assertTrue(job in CI.JOB_CONFIGS)
            self.assertTrue(CI.JOB_CONFIGS[job].runner_type in CI.Runners)
            if job in CI.BuildNames:
                self.assertTrue(CI.JOB_CONFIGS[job].build_config.name == job)
                self.assertTrue(CI.JOB_CONFIGS[job].required_builds is None)
            else:
                self.assertTrue(CI.JOB_CONFIGS[job].build_config is None)
                if "asan" in job and "aarch" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_ARM_ASAN]
                elif "asan" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_ASAN]
                elif "msan" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_MSAN]
                elif "tsan" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_TSAN]
                elif "ubsan" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_UBSAN]
                elif "debug" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_DEBUG]
                elif job in (
                    "Unit tests (release)",
                    "ClickHouse Keeper Jepsen",
                    "ClickHouse Server Jepsen",
                ):
                    expected_builds = [CI.BuildNames.BINARY_RELEASE]
                elif "release" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_RELEASE]
                elif "coverage" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_RELEASE_COVERAGE]
                elif "aarch" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_AARCH64]
                elif "amd64" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_RELEASE]
                elif "uzzer" in job:
                    expected_builds = [CI.BuildNames.FUZZERS]
                elif "Docker" in job:
                    expected_builds = [
                        CI.BuildNames.PACKAGE_RELEASE,
                        CI.BuildNames.PACKAGE_AARCH64,
                    ]
                    self.assertTrue(
                        CI.JOB_CONFIGS[job].required_builds[0]
                        in (
                            CI.BuildNames.PACKAGE_RELEASE,
                            CI.BuildNames.PACKAGE_AARCH64,
                        ),
                        f"Job [{job}] probably has wrong required build [{CI.JOB_CONFIGS[job].required_builds[0]}] in JobConfig",
                    )
                elif "SQLTest" in job:
                    expected_builds = [CI.BuildNames.PACKAGE_RELEASE]
                elif "Jepsen" in job:
                    expected_builds = [
                        CI.BuildNames.PACKAGE_RELEASE,
                        CI.BuildNames.BINARY_RELEASE,
                    ]
                elif job in (
                    CI.JobNames.STYLE_CHECK,
                    CI.JobNames.FAST_TEST,
                    CI.JobNames.BUILD_CHECK,
                    CI.JobNames.DOCS_CHECK,
                    CI.JobNames.BUGFIX_VALIDATE,
                ):
                    expected_builds = []
                else:
                    print(f"Job [{job}] required build not checked")
                    assert False

                self.assertCountEqual(
                    expected_builds,
                    CI.JOB_CONFIGS[job].required_builds or [],
                    f"Required builds are not valid for job [{job}]",
                )

    def test_job_stage_config(self):
        """
        check runner is provided w/o exception
        """
        # check stages
        for job in CI.JobNames:
            if job in CI.BuildNames:
                self.assertTrue(
                    CI.get_job_ci_stage(job)
                    in (CI.WorkflowStages.BUILDS_1, CI.WorkflowStages.BUILDS_2)
                )
            else:
                if job in (
                    CI.JobNames.STYLE_CHECK,
                    CI.JobNames.FAST_TEST,
                    CI.JobNames.JEPSEN_SERVER,
                    CI.JobNames.JEPSEN_KEEPER,
                    CI.JobNames.BUILD_CHECK,
                ):
                    self.assertEqual(
                        CI.get_job_ci_stage(job),
                        CI.WorkflowStages.NA,
                        msg=f"Stage for [{job}] is not correct",
                    )
                else:
                    self.assertTrue(
                        CI.get_job_ci_stage(job)
                        in (CI.WorkflowStages.TESTS_1, CI.WorkflowStages.TESTS_2),
                        msg=f"Stage for [{job}] is not correct",
                    )

    def test_job_stage_config_non_blocking(self):
        """
        check runner is provided w/o exception
        """
        # check stages
        for job in CI.JobNames:
            if job in CI.BuildNames:
                self.assertTrue(
                    CI.get_job_ci_stage(job)
                    in (CI.WorkflowStages.BUILDS_1, CI.WorkflowStages.BUILDS_2)
                )
            else:
                if job in (
                    CI.JobNames.STYLE_CHECK,
                    CI.JobNames.FAST_TEST,
                    CI.JobNames.JEPSEN_SERVER,
                    CI.JobNames.JEPSEN_KEEPER,
                    CI.JobNames.BUILD_CHECK,
                ):
                    self.assertEqual(
                        CI.get_job_ci_stage(job),
                        CI.WorkflowStages.NA,
                        msg=f"Stage for [{job}] is not correct",
                    )
                else:
                    self.assertTrue(
                        CI.get_job_ci_stage(job, non_blocking_ci=True)
                        in (CI.WorkflowStages.TESTS_1, CI.WorkflowStages.TESTS_2_WW),
                        msg=f"Stage for [{job}] is not correct",
                    )

    def test_build_jobs_configs(self):
        """
        check build jobs have non-None build_config attribute
        check test jobs have None build_config attribute
        """
        for job in CI.JobNames:
            if job in CI.BuildNames:
                self.assertTrue(
                    isinstance(CI.JOB_CONFIGS[job].build_config, CI.BuildConfig)
                )
            else:
                self.assertTrue(CI.JOB_CONFIGS[job].build_config is None)

    def test_ci_py_for_pull_request(self):
        """
        checks ci.py job configuration
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        settings.ci_sets = [CI.Tags.CI_SET_BUILDS]
        settings.include_keywords = [
            "package",
            "integration",
            "upgrade",
            "clickHouse_build_check",
            "stateless",
        ]
        settings.exclude_keywords = ["asan", "aarch64"]
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        # make it pull request info
        pr_info.event_type = EventType.PULL_REQUEST
        pr_info.number = 12345
        assert pr_info.is_pr and not pr_info.is_release and not pr_info.is_master
        assert not pr_info.is_merge_queue
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        ci_cache.filter_out_not_affected_jobs()
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = []
        for set_ in settings.ci_sets:
            tag_config = CI.get_tag_config(set_)
            assert tag_config
            set_jobs = tag_config.run_jobs
            for job in set_jobs:
                if any(
                    k in Utils.normalize_string(job) for k in settings.exclude_keywords
                ):
                    continue
                expected_jobs_to_do.append(job)
        for job, config in CI.JOB_CONFIGS.items():
            if (
                CI.is_build_job(job)
                and not config.run_by_labels
                and job not in expected_jobs_to_do
            ):
                # expected to run all builds jobs
                expected_jobs_to_do.append(job)
            if not any(
                keyword in Utils.normalize_string(job)
                for keyword in settings.include_keywords
            ):
                continue
            if any(
                keyword in Utils.normalize_string(job)
                for keyword in settings.exclude_keywords
            ):
                continue
            if config.random_bucket:
                continue
            if job not in expected_jobs_to_do:
                expected_jobs_to_do.append(job)

        random_buckets = []
        for job, config in ci_cache.jobs_to_do.items():
            if config.random_bucket:
                self.assertTrue(
                    config.random_bucket not in random_buckets,
                    "Only one job must be picked up from each random bucket",
                )
                random_buckets.append(config.random_bucket)
                actual_jobs_to_do.remove(job)

        self.assertCountEqual(expected_jobs_to_do, actual_jobs_to_do)

    def test_ci_py_for_pull_request_no_settings(self):
        """
        checks ci.py job configuration in PR with empty ci_settings
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        # make it pull request info
        pr_info.event_type = EventType.PULL_REQUEST
        pr_info.number = 12345
        assert pr_info.is_pr and not pr_info.is_release and not pr_info.is_master
        assert not pr_info.is_merge_queue
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = []
        for job, config in CI.JOB_CONFIGS.items():
            if config.random_bucket:
                continue
            if config.release_only:
                continue
            if config.run_by_labels:
                continue
            expected_jobs_to_do.append(job)

        random_buckets = []
        for job, config in ci_cache.jobs_to_do.items():
            if config.random_bucket:
                self.assertTrue(
                    config.random_bucket not in random_buckets,
                    "Only one job must be picked up from each random bucket",
                )
                random_buckets.append(config.random_bucket)
                actual_jobs_to_do.remove(job)

        self.assertCountEqual(expected_jobs_to_do, actual_jobs_to_do)

    def test_ci_py_for_master(self):
        """
        checks ci.py job configuration
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PUSH
        assert pr_info.number == 0 and pr_info.is_release and not pr_info.is_merge_queue
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = []
        for job, config in CI.JOB_CONFIGS.items():
            if config.pr_only:
                continue
            if config.run_by_labels:
                continue
            if job in CI.MQ_JOBS:
                continue
            expected_jobs_to_do.append(job)
        self.assertCountEqual(expected_jobs_to_do, actual_jobs_to_do)

    def test_ci_py_for_merge_queue(self):
        """
        checks ci.py job configuration
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        # make it merge_queue
        pr_info.event_type = EventType.MERGE_QUEUE
        assert (
            pr_info.number == 0
            and pr_info.is_merge_queue
            and not pr_info.is_release
            and not pr_info.is_master
            and not pr_info.is_pr
        )
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = [
            "Style check",
            "Fast test",
            "binary_release",
            "Unit tests (release)",
        ]
        self.assertCountEqual(expected_jobs_to_do, actual_jobs_to_do)

    def test_ci_py_for_specific_workflow(self):
        """
        checks ci.py job configuration
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        # make it merge_queue
        pr_info.event_type = EventType.SCHEDULE
        assert pr_info.number == 0 and not pr_info.is_merge_queue and not pr_info.is_pr
        ci_cache = CIPY._configure_jobs(
            S3Helper(),
            pr_info,
            settings,
            skip_jobs=False,
            dry_run=True,
            workflow_name=CI.WorkFlowNames.JEPSEN,
        )
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = [
            CI.BuildNames.BINARY_RELEASE,
            CI.JobNames.JEPSEN_KEEPER,
            CI.JobNames.JEPSEN_SERVER,
        ]
        self.assertCountEqual(expected_jobs_to_do, actual_jobs_to_do)

    def test_ci_py_await(self):
        """
        checks ci.py job configuration
        """

        def _reset_ci_cache_to_wait_all_jobs(ci_cache):
            # pretend there are pending jobs that we need to wait
            ci_cache.jobs_to_wait = dict(ci_cache.jobs_to_do)
            for job, config in ci_cache.jobs_to_wait.items():
                assert config.batches
                config.pending_batches = list(config.batches)

                for batch in range(config.num_batches):
                    record = CiCache.Record(
                        record_type=CiCache.RecordType.PENDING,
                        job_name=job,
                        job_digest=ci_cache.job_digests[job],
                        batch=batch,
                        num_batches=config.num_batches,
                        release_branch=True,
                    )
                    for record_t_, records_ in ci_cache.records.items():
                        if record_t_.value == CiCache.RecordType.PENDING.value:
                            records_[record.to_str_key()] = record
                    assert not ci_cache.jobs_to_skip
            assert ci_cache.jobs_to_wait
            ci_cache.jobs_to_skip = []

        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PUSH
        pr_info.number = 0
        pr_info.head_ref = "24.12345"
        assert pr_info.is_release and not pr_info.is_merge_queue
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        self.assertTrue(not ci_cache.jobs_to_skip, "Must be no jobs in skip list")
        all_jobs_in_wf = list(ci_cache.jobs_to_do)
        assert not ci_cache.jobs_to_wait
        ci_cache.await_pending_jobs(is_release=pr_info.is_release, dry_run=True)
        assert not ci_cache.jobs_to_skip
        assert not ci_cache.jobs_to_wait

        def _test_await_for_batch(
            ci_cache: CiCache, record_type: CiCache.RecordType, batch: int
        ) -> None:
            assert ci_cache.jobs_to_wait
            for job_, config_ in ci_cache.jobs_to_wait.items():
                record = CiCache.Record(
                    record_type=record_type,
                    job_name=job_,
                    job_digest=ci_cache.job_digests[job_],
                    batch=batch,
                    num_batches=config_.num_batches,
                    release_branch=True,
                )
                for record_t_, records_ in ci_cache.records.items():
                    if record_t_.value == record_type.value:
                        records_[record.to_str_key()] = record
            # await
            ci_cache.await_pending_jobs(is_release=pr_info.is_release, dry_run=True)
            for _, config_ in ci_cache.jobs_to_wait.items():
                assert config_.pending_batches
                if (
                    record_type != CiCache.RecordType.PENDING
                    and batch < config_.num_batches
                ):
                    assert batch not in config_.pending_batches

            for _, config_ in ci_cache.jobs_to_do.items():
                # jobs to do must have batches to run before/after await
                #  if it's an empty list after await - apparently job has not been removed after await
                assert config_.batches

        _reset_ci_cache_to_wait_all_jobs(ci_cache)
        _test_await_for_batch(ci_cache, CiCache.RecordType.FAILED, 0)
        tested = False
        for job, config in ci_cache.jobs_to_do.items():
            if config.batches == [0]:
                tested = True
                self.assertTrue(
                    job not in ci_cache.jobs_to_wait,
                    "Job must be removed from @jobs_to_wait, because its only batch has FAILED cache record",
                )
        self.assertCountEqual(
            ci_cache.jobs_to_skip,
            [],
            "No jobs must be skipped, since all cache records are of type FAILED",
        )
        assert tested

        # reset jobs_to_wait after previous test
        _reset_ci_cache_to_wait_all_jobs(ci_cache)
        assert not ci_cache.jobs_to_skip

        # set batch 0 as SUCCESSFUL in ci cache
        jobs_to_do_prev = list(ci_cache.jobs_to_do)
        jobs_to_skip_prev = []
        jobs_to_wait_prev = list(ci_cache.jobs_to_wait)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 0)
        self.assertTrue(len(jobs_to_skip_prev) != len(ci_cache.jobs_to_skip))
        self.assertTrue(len(jobs_to_wait_prev) > len(ci_cache.jobs_to_wait))
        self.assertCountEqual(
            list(ci_cache.jobs_to_do) + ci_cache.jobs_to_skip,
            jobs_to_do_prev + jobs_to_skip_prev,
        )

        # set batch 1 as SUCCESSFUL in ci cache
        jobs_to_do_prev = list(ci_cache.jobs_to_do)
        jobs_to_skip_prev = list(ci_cache.jobs_to_skip)
        jobs_to_wait_prev = list(ci_cache.jobs_to_wait)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 1)
        self.assertTrue(len(jobs_to_skip_prev) != len(ci_cache.jobs_to_skip))
        self.assertTrue(len(jobs_to_wait_prev) > len(ci_cache.jobs_to_wait))
        self.assertCountEqual(
            list(ci_cache.jobs_to_do) + ci_cache.jobs_to_skip,
            jobs_to_do_prev + jobs_to_skip_prev,
        )

        # set batch 3, 4, 5, 6 as SUCCESSFUL in ci cache
        jobs_to_do_prev = list(ci_cache.jobs_to_do)
        jobs_to_skip_prev = list(ci_cache.jobs_to_skip)
        jobs_to_wait_prev = list(ci_cache.jobs_to_wait)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 2)
        self.assertTrue(ci_cache.jobs_to_do)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 3)
        self.assertTrue(ci_cache.jobs_to_do)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 4)
        self.assertTrue(ci_cache.jobs_to_do)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 5)
        self.assertTrue(
            not ci_cache.jobs_to_do
        )  # by this moment there must be no jobs left as batch 5 is currently the maximum
        self.assertTrue(len(jobs_to_skip_prev) != len(ci_cache.jobs_to_skip))
        self.assertTrue(len(jobs_to_wait_prev) > len(ci_cache.jobs_to_wait))
        self.assertCountEqual(
            list(ci_cache.jobs_to_do) + ci_cache.jobs_to_skip,
            jobs_to_do_prev + jobs_to_skip_prev,
        )

    def test_ci_py_filters_not_affected_jobs_in_prs(self):
        """
        checks ci.py filters not affected jobs in PRs
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PULL_REQUEST
        pr_info.number = 123
        assert pr_info.is_pr
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        self.assertTrue(not ci_cache.jobs_to_skip, "Must be no jobs in skip list")
        assert not ci_cache.jobs_to_wait
        assert not ci_cache.jobs_to_skip

        MOCK_AFFECTED_JOBS = [
            CI.JobNames.STATELESS_TEST_S3_DEBUG,
            CI.JobNames.STRESS_TEST_TSAN,
        ]
        MOCK_REQUIRED_BUILDS = []

        # pretend there are pending jobs that we need to wait
        for job, job_config in ci_cache.jobs_to_do.items():
            if job in MOCK_AFFECTED_JOBS:
                MOCK_REQUIRED_BUILDS += job_config.required_builds
            elif job not in MOCK_AFFECTED_JOBS and not job_config.disable_await:
                ci_cache.jobs_to_wait[job] = job_config

        for job, job_config in ci_cache.jobs_to_do.items():
            if job_config.reference_job_name or job_config.disable_await:
                # jobs with reference_job_name in config are not supposed to have records in the cache - continue
                continue
            if job in MOCK_AFFECTED_JOBS:
                continue
            for batch in range(job_config.num_batches):
                # add any record into cache
                record = CiCache.Record(
                    record_type=random.choice(
                        [
                            CiCache.RecordType.FAILED,
                            CiCache.RecordType.PENDING,
                            CiCache.RecordType.SUCCESSFUL,
                        ]
                    ),
                    job_name=job,
                    job_digest=ci_cache.job_digests[job],
                    batch=batch,
                    num_batches=job_config.num_batches,
                    release_branch=True,
                )
                for record_t_, records_ in ci_cache.records.items():
                    if record_t_.value == record.record_type.value:
                        records_[record.to_str_key()] = record

        ci_cache.filter_out_not_affected_jobs()
        expected_to_do = (
            [
                CI.JobNames.BUILD_CHECK,
            ]
            + MOCK_AFFECTED_JOBS
            + MOCK_REQUIRED_BUILDS
        )
        self.assertTrue(
            CI.JobNames.BUILD_CHECK not in ci_cache.jobs_to_wait,
            "We must never await on Builds Report",
        )
        self.assertCountEqual(
            list(ci_cache.jobs_to_wait),
            MOCK_REQUIRED_BUILDS,
        )
        self.assertCountEqual(list(ci_cache.jobs_to_do), expected_to_do)

    def test_ci_py_filters_not_affected_jobs_in_prs_no_builds(self):
        """
        checks ci.py filters not affected jobs in PRs, no builds required
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PULL_REQUEST
        pr_info.number = 123
        assert pr_info.is_pr
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        self.assertTrue(not ci_cache.jobs_to_skip, "Must be no jobs in skip list")
        assert not ci_cache.jobs_to_wait
        assert not ci_cache.jobs_to_skip

        MOCK_AFFECTED_JOBS = [
            CI.JobNames.FAST_TEST,
        ]
        MOCK_REQUIRED_BUILDS = []

        # pretend there are pending jobs that we need to wait
        for job, job_config in ci_cache.jobs_to_do.items():
            if job in MOCK_AFFECTED_JOBS:
                if job_config.required_builds:
                    MOCK_REQUIRED_BUILDS += job_config.required_builds
            elif job not in MOCK_AFFECTED_JOBS and not job_config.disable_await:
                ci_cache.jobs_to_wait[job] = job_config

        for job, job_config in ci_cache.jobs_to_do.items():
            if job_config.reference_job_name or job_config.disable_await:
                # jobs with reference_job_name in config are not supposed to have records in the cache - continue
                continue
            if job in MOCK_AFFECTED_JOBS:
                continue
            for batch in range(job_config.num_batches):
                # add any record into cache
                record = CiCache.Record(
                    record_type=random.choice(
                        [
                            CiCache.RecordType.FAILED,
                            CiCache.RecordType.PENDING,
                            CiCache.RecordType.SUCCESSFUL,
                        ]
                    ),
                    job_name=job,
                    job_digest=ci_cache.job_digests[job],
                    batch=batch,
                    num_batches=job_config.num_batches,
                    release_branch=True,
                )
                for record_t_, records_ in ci_cache.records.items():
                    if record_t_.value == record.record_type.value:
                        records_[record.to_str_key()] = record

        ci_cache.filter_out_not_affected_jobs()
        expected_to_do = MOCK_AFFECTED_JOBS + MOCK_REQUIRED_BUILDS
        self.assertCountEqual(
            list(ci_cache.jobs_to_wait),
            MOCK_REQUIRED_BUILDS,
        )
        self.assertCountEqual(list(ci_cache.jobs_to_do), expected_to_do)

    def test_ci_py_filters_not_affected_jobs_in_prs_docs_check(self):
        """
        checks ci.py filters not affected jobs in PRs,
        Docs Check is special from ci_cache perspective -
            check it ci pr pipline is filtered properly when only docs check is to be skipped
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PULL_REQUEST
        pr_info.number = 123
        assert pr_info.is_pr
        ci_cache = CIPY._configure_jobs(
            S3Helper(), pr_info, settings, skip_jobs=False, dry_run=True
        )
        self.assertTrue(not ci_cache.jobs_to_skip, "Must be no jobs in skip list")
        assert not ci_cache.jobs_to_wait
        assert not ci_cache.jobs_to_skip

        job_config = ci_cache.jobs_to_do[CI.JobNames.DOCS_CHECK]
        for batch in range(job_config.num_batches):
            # add any record into cache
            record = CiCache.Record(
                record_type=CiCache.RecordType.PENDING,
                job_name=CI.JobNames.DOCS_CHECK,
                job_digest=ci_cache.job_digests[CI.JobNames.DOCS_CHECK],
                batch=batch,
                num_batches=job_config.num_batches,
                release_branch=True,
            )
            for record_t_, records_ in ci_cache.records.items():
                if record_t_.value == record.record_type.value:
                    records_[record.to_str_key()] = record

        expected_jobs = list(ci_cache.jobs_to_do)
        expected_jobs.remove(CI.JobNames.DOCS_CHECK)
        ci_cache.filter_out_not_affected_jobs()
        self.assertCountEqual(list(ci_cache.jobs_to_do), expected_jobs)
