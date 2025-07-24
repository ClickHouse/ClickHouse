#!/usr/bin/env python

import shutil
import unittest
from hashlib import md5
from pathlib import Path
from typing import Dict, Set

from ci_cache import CiCache
from ci_config import CI
from commit_status_helper import CommitStatusData
from digest_helper import JOB_DIGEST_LEN
from env_helper import S3_BUILDS_BUCKET, TEMP_PATH
from s3_helper import S3Helper


def _create_mock_digest_1(string):
    return md5((string).encode("utf-8")).hexdigest()[:JOB_DIGEST_LEN]


def _create_mock_digest_2(string):
    return md5((string + "+nonce").encode("utf-8")).hexdigest()[:JOB_DIGEST_LEN]


DIGESTS = {job: _create_mock_digest_1(job) for job in CI.JobNames}
DIGESTS2 = {job: _create_mock_digest_2(job) for job in CI.JobNames}


# pylint:disable=protected-access
class S3HelperTestMock(S3Helper):
    def __init__(self) -> None:
        super().__init__()
        self.files_on_s3_paths = {}  # type: Dict[str, Set[str]]

        # local path which is mocking remote s3 path with ci_cache
        self.mock_remote_s3_path = Path(TEMP_PATH) / "mock_s3_path"
        if not self.mock_remote_s3_path.exists():
            self.mock_remote_s3_path.mkdir(parents=True, exist_ok=True)
        for file in self.mock_remote_s3_path.iterdir():
            file.unlink()

    def list_prefix(self, s3_prefix_path, bucket=S3_BUILDS_BUCKET):
        assert bucket == S3_BUILDS_BUCKET
        file_prefix = Path(s3_prefix_path).name
        path = str(Path(s3_prefix_path).parent)
        return [
            path + "/" + file
            for file in self.files_on_s3_paths[path]
            if file.startswith(file_prefix)
        ]

    def upload_file(self, bucket, file_path, s3_path):
        assert bucket == S3_BUILDS_BUCKET
        file_name = Path(file_path).name
        assert (
            file_name in s3_path
        ), f"Record file name [{file_name}] must be part of a path on s3 [{s3_path}]"
        s3_path = str(Path(s3_path).parent)
        if s3_path in self.files_on_s3_paths:
            self.files_on_s3_paths[s3_path].add(file_name)
        else:
            self.files_on_s3_paths[s3_path] = set([file_name])
        shutil.copy(file_path, self.mock_remote_s3_path)

    def download_files(self, bucket, s3_path, file_suffix, local_directory):
        assert bucket == S3_BUILDS_BUCKET
        assert file_suffix == CiCache._RECORD_FILE_EXTENSION
        assert local_directory == CiCache._LOCAL_CACHE_PATH
        assert CiCache._S3_CACHE_PREFIX in s3_path
        assert [job_type.value in s3_path for job_type in CiCache.JobType]

        # copying from mock remote path to local cache
        for remote_record in self.mock_remote_s3_path.glob(f"*{file_suffix}"):
            destination_file = CiCache._LOCAL_CACHE_PATH / remote_record.name
            shutil.copy(remote_record, destination_file)


# pylint:disable=protected-access
class TestCiCache(unittest.TestCase):
    def test_cache(self):
        s3_mock = S3HelperTestMock()
        ci_cache = CiCache(s3_mock, DIGESTS)
        # immitate another CI run is using cache
        ci_cache_2 = CiCache(s3_mock, DIGESTS2)
        NUM_BATCHES = 10

        DOCS_JOBS_NUM = 1
        assert len(set(job for job in CI.JobNames)) == len(
            list(job for job in CI.JobNames)
        )
        NONDOCS_JOBS_NUM = len(set(job for job in CI.JobNames)) - DOCS_JOBS_NUM

        PR_NUM = 123456
        status = CommitStatusData(
            status="success",
            report_url="dummy url",
            description="OK OK OK",
            sha="deadbeaf2",
            pr_num=PR_NUM,
        )

        ### add some pending statuses for two batches, non-release branch
        for job in CI.JobNames:
            ci_cache.push_pending(job, [0, 1, 2], NUM_BATCHES, release_branch=False)
            ci_cache_2.push_pending(job, [0, 1, 2], NUM_BATCHES, release_branch=False)

        ### add success status for 0 batch, non-release branch
        batch = 0
        for job in CI.JobNames:
            ci_cache.push_successful(
                job, batch, NUM_BATCHES, status, release_branch=False
            )
            ci_cache_2.push_successful(
                job, batch, NUM_BATCHES, status, release_branch=False
            )

        ### add failed status for 2 batch, non-release branch
        batch = 2
        for job in CI.JobNames:
            ci_cache.push_failed(job, batch, NUM_BATCHES, status, release_branch=False)
            ci_cache_2.push_failed(
                job, batch, NUM_BATCHES, status, release_branch=False
            )

        ### check all expected directories were created on s3 mock
        expected_build_path_1 = f"{CiCache.JobType.SRCS.value}-{_create_mock_digest_1(CI.BuildNames.PACKAGE_RELEASE)}"
        expected_docs_path_1 = f"{CiCache.JobType.DOCS.value}-{_create_mock_digest_1(CI.JobNames.DOCS_CHECK)}"
        expected_build_path_2 = f"{CiCache.JobType.SRCS.value}-{_create_mock_digest_2(CI.BuildNames.PACKAGE_RELEASE)}"
        expected_docs_path_2 = f"{CiCache.JobType.DOCS.value}-{_create_mock_digest_2(CI.JobNames.DOCS_CHECK)}"
        self.assertCountEqual(
            list(s3_mock.files_on_s3_paths.keys()),
            [
                f"{CiCache._S3_CACHE_PREFIX}/{expected_build_path_1}",
                f"{CiCache._S3_CACHE_PREFIX}/{expected_docs_path_1}",
                f"{CiCache._S3_CACHE_PREFIX}/{expected_build_path_2}",
                f"{CiCache._S3_CACHE_PREFIX}/{expected_docs_path_2}",
            ],
        )

        ### check number of cache files is as expected
        FILES_PER_JOB = 5  # 1 successful + 1 failed + 3 pending batches = 5
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_build_path_1}"
                ]
            ),
            NONDOCS_JOBS_NUM * FILES_PER_JOB,
        )
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_docs_path_1}"
                ]
            ),
            DOCS_JOBS_NUM * FILES_PER_JOB,
        )
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_build_path_2}"
                ]
            ),
            NONDOCS_JOBS_NUM * FILES_PER_JOB,
        )
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_docs_path_2}"
                ]
            ),
            DOCS_JOBS_NUM * FILES_PER_JOB,
        )

        ### check statuses for all jobs in cache
        for job in CI.JobNames:
            self.assertEqual(
                ci_cache.is_successful(job, 0, NUM_BATCHES, release_branch=False), True
            )
            self.assertEqual(
                ci_cache.is_successful(job, 0, NUM_BATCHES, release_branch=True), False
            )
            self.assertEqual(
                ci_cache.is_successful(
                    job, batch=1, num_batches=NUM_BATCHES, release_branch=False
                ),
                False,
            )  # false - it's pending
            self.assertEqual(
                ci_cache.is_successful(
                    job,
                    batch=NUM_BATCHES,
                    num_batches=NUM_BATCHES,
                    release_branch=False,
                ),
                False,
            )  # false - no such record
            self.assertEqual(
                ci_cache.is_pending(job, 0, NUM_BATCHES, release_branch=False), False
            )  # false, it's successful, success has more priority than pending
            self.assertEqual(
                ci_cache.is_pending(job, 1, NUM_BATCHES, release_branch=False), True
            )  # true
            self.assertEqual(
                ci_cache.is_pending(job, 1, NUM_BATCHES, release_branch=True), False
            )  # false, not pending job on release_branch

            status2 = ci_cache.get_successful(job, 0, NUM_BATCHES)
            assert status2 and status2.pr_num == PR_NUM
            status2 = ci_cache.get_successful(job, 1, NUM_BATCHES)
            assert status2 is None

        ### add some more pending statuses for two batches and for a release branch
        for job in CI.JobNames:
            ci_cache.push_pending(
                job, batches=[0, 1], num_batches=NUM_BATCHES, release_branch=True
            )

        ### add success statuses for 0 batch and release branch
        PR_NUM = 234
        status = CommitStatusData(
            status="success",
            report_url="dummy url",
            description="OK OK OK",
            sha="deadbeaf2",
            pr_num=PR_NUM,
        )
        for job in CI.JobNames:
            ci_cache.push_successful(job, 0, NUM_BATCHES, status, release_branch=True)

        ### check number of cache files is as expected
        FILES_PER_JOB = 8  # 1 successful + 1 failed + 1 successful_release + 3 pending batches + 2 pending batches release = 8
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_build_path_1}"
                ]
            ),
            NONDOCS_JOBS_NUM * FILES_PER_JOB,
        )
        self.assertEqual(
            len(
                s3_mock.files_on_s3_paths[
                    f"{CiCache._S3_CACHE_PREFIX}/{expected_docs_path_1}"
                ]
            ),
            DOCS_JOBS_NUM * FILES_PER_JOB,
        )

        ### check statuses
        for job in CI.JobNames:
            self.assertEqual(ci_cache.is_successful(job, 0, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_successful(job, 0, NUM_BATCHES, True), True)
            self.assertEqual(ci_cache.is_successful(job, 1, NUM_BATCHES, False), False)
            self.assertEqual(ci_cache.is_successful(job, 1, NUM_BATCHES, True), False)
            self.assertEqual(
                ci_cache.is_pending(job, 0, NUM_BATCHES, False), False
            )  # it's success, not pending
            self.assertEqual(
                ci_cache.is_pending(job, 0, NUM_BATCHES, True), False
            )  # it's success, not pending
            self.assertEqual(ci_cache.is_pending(job, 1, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_pending(job, 1, NUM_BATCHES, True), True)

            self.assertEqual(ci_cache.is_failed(job, 2, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_failed(job, 2, NUM_BATCHES, True), False)

            status2 = ci_cache.get_successful(job, 0, NUM_BATCHES)
            assert status2 and status2.pr_num == PR_NUM
            status2 = ci_cache.get_successful(job, 1, NUM_BATCHES)
            assert status2 is None

        ### create new cache object and verify the same checks
        ci_cache = CiCache(s3_mock, DIGESTS)
        for job in CI.JobNames:
            self.assertEqual(ci_cache.is_successful(job, 0, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_successful(job, 0, NUM_BATCHES, True), True)
            self.assertEqual(ci_cache.is_successful(job, 1, NUM_BATCHES, False), False)
            self.assertEqual(ci_cache.is_successful(job, 1, NUM_BATCHES, True), False)
            self.assertEqual(
                ci_cache.is_pending(job, 0, NUM_BATCHES, False), False
            )  # it's success, not pending
            self.assertEqual(
                ci_cache.is_pending(job, 0, NUM_BATCHES, True), False
            )  # it's success, not pending
            self.assertEqual(ci_cache.is_pending(job, 1, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_pending(job, 1, NUM_BATCHES, True), True)

            self.assertEqual(ci_cache.is_failed(job, 2, NUM_BATCHES, False), True)
            self.assertEqual(ci_cache.is_failed(job, 2, NUM_BATCHES, True), False)

            # is_pending() is false for failed jobs batches
            self.assertEqual(ci_cache.is_pending(job, 2, NUM_BATCHES, False), False)
            self.assertEqual(ci_cache.is_pending(job, 2, NUM_BATCHES, True), False)

            status2 = ci_cache.get_successful(job, 0, NUM_BATCHES)
            assert status2 and status2.pr_num == PR_NUM
            status2 = ci_cache.get_successful(job, 1, NUM_BATCHES)
            assert status2 is None

        ### check some job values which are not in the cache
        self.assertEqual(ci_cache.is_successful(job, 0, NUM_BATCHES + 1, False), False)
        self.assertEqual(
            ci_cache.is_successful(job, NUM_BATCHES - 1, NUM_BATCHES, False), False
        )
        self.assertEqual(ci_cache.is_pending(job, 0, NUM_BATCHES + 1, False), False)
        self.assertEqual(
            ci_cache.is_pending(job, NUM_BATCHES - 1, NUM_BATCHES, False), False
        )


if __name__ == "__main__":
    TestCiCache().test_cache()
