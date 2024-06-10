#!/usr/bin/env python3

import unittest
from ci_config import CI
import ci as CIPY
from ci_settings import CiSettings
from pr_info import PRInfo, EventType
from s3_helper import S3Helper
from ci_cache import CiCache
from ci_utils import normalize_string


_TEST_EVENT_JSON = {"dummy": "dummy"}

# pylint:disable=protected-access


class TestCIConfig(unittest.TestCase):
    def test_runner_config(self):
        """check runner is provided w/o exception"""
        for job in CI.JobNames:
            self.assertIn(CI.JOB_CONFIGS[job].runner_type, CI.Runners)

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
                        in (CI.WorkflowStages.TESTS_1, CI.WorkflowStages.TESTS_3),
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
        actual_jobs_to_do = list(ci_cache.jobs_to_do)
        expected_jobs_to_do = []
        for set_ in settings.ci_sets:
            tag_config = CI.get_tag_config(set_)
            assert tag_config
            set_jobs = tag_config.run_jobs
            for job in set_jobs:
                if any(k in normalize_string(job) for k in settings.exclude_keywords):
                    continue
                expected_jobs_to_do.append(job)
        for job, config in CI.JOB_CONFIGS.items():
            if not any(
                keyword in normalize_string(job)
                for keyword in settings.include_keywords
            ):
                continue
            if any(
                keyword in normalize_string(job)
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
            if config.run_by_label:
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
            if config.run_by_label:
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

    def test_ci_py_await(self):
        """
        checks ci.py job configuration
        """
        settings = CiSettings()
        settings.no_ci_cache = True
        pr_info = PRInfo(github_event=_TEST_EVENT_JSON)
        pr_info.event_type = EventType.PUSH
        pr_info.number = 0
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

        # pretend there are pending jobs that we neet to wait
        ci_cache.jobs_to_wait = dict(ci_cache.jobs_to_do)
        for job, config in ci_cache.jobs_to_wait.items():
            assert not config.pending_batches
            assert config.batches
            config.pending_batches = list(config.batches)
        for job, config in ci_cache.jobs_to_wait.items():
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
                else:
                    assert batch in config_.pending_batches

        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 0)
        # check all one-batch jobs are in jobs_to_skip
        for job in all_jobs_in_wf:
            config = CI.JOB_CONFIGS[job]
            if config.num_batches == 1:
                self.assertTrue(job in ci_cache.jobs_to_skip)
                self.assertTrue(job not in ci_cache.jobs_to_do)
            else:
                self.assertTrue(job not in ci_cache.jobs_to_skip)
                self.assertTrue(job in ci_cache.jobs_to_do)

        _test_await_for_batch(ci_cache, CiCache.RecordType.FAILED, 1)
        _test_await_for_batch(ci_cache, CiCache.RecordType.SUCCESSFUL, 2)

        self.assertTrue(len(ci_cache.jobs_to_skip) > 0)
        self.assertTrue(len(ci_cache.jobs_to_do) > 0)
        self.assertCountEqual(
            list(ci_cache.jobs_to_do) + ci_cache.jobs_to_skip, all_jobs_in_wf
        )
