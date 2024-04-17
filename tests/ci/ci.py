import argparse
import concurrent.futures
import json
import logging
import os
import random
import re
import subprocess
import sys
import time
from copy import deepcopy
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import docker_images_helper
import upload_result_helper
from build_check import get_release_or_pr
from ci_config import CI_CONFIG, Build, CIStages, JobNames, Labels
from ci_utils import GHActions, is_hex, normalize_string
from clickhouse_helper import (
    CiLogsCredentials,
    ClickHouseHelper,
    InsertException,
    get_instance_id,
    get_instance_type,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    CommitStatusData,
    RerunHelper,
    format_description,
    get_commit,
    post_commit_status,
    set_status_comment,
    update_mergeable_check,
)
from digest_helper import DockerDigester, JobDigester
from env_helper import (
    CI,
    GITHUB_JOB_API_URL,
    GITHUB_RUN_URL,
    REPO_COPY,
    REPORT_PATH,
    S3_BUILDS_BUCKET,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, Git
from git_helper import Runner as GitRunner
from github_helper import GitHub
from pr_info import PRInfo
from report import ERROR, SUCCESS, BuildResult, JobReport
from s3_helper import S3Helper
from version_helper import get_version_from_repo

# pylint: disable=too-many-lines


@dataclass
class PendingState:
    updated_at: float
    run_url: str


class CiCache:
    """
    CI cache is a bunch of records. Record is a file stored under special location on s3.
    The file name has following format

        <RECORD_TYPE>_[<ATTRIBUTES>]--<JOB_NAME>_<JOB_DIGEST>_<BATCH>_<NUM_BATCHES>.ci

    RECORD_TYPE:
        SUCCESSFUL - for successfuly finished jobs
        PENDING - for pending jobs

    ATTRIBUTES:
        release - for jobs being executed on the release branch including master branch (not a PR branch)
    """

    _S3_CACHE_PREFIX = "CI_cache_v1"
    _CACHE_BUILD_REPORT_PREFIX = "build_report"
    _RECORD_FILE_EXTENSION = ".ci"
    _LOCAL_CACHE_PATH = Path(TEMP_PATH) / "ci_cache"
    _ATTRIBUTE_RELEASE = "release"
    # divider symbol 1
    _DIV1 = "--"
    # divider symbol 2
    _DIV2 = "_"
    assert _DIV1 != _DIV2

    class RecordType(Enum):
        SUCCESSFUL = "successful"
        PENDING = "pending"
        FAILED = "failed"

    @dataclass
    class Record:
        record_type: "CiCache.RecordType"
        job_name: str
        job_digest: str
        batch: int
        num_batches: int
        release_branch: bool
        file: str = ""

        def to_str_key(self):
            """other fields must not be included in the hash str"""
            return "_".join(
                [self.job_name, self.job_digest, str(self.batch), str(self.num_batches)]
            )

    class JobType(Enum):
        DOCS = "DOCS"
        SRCS = "SRCS"

        @classmethod
        def is_docs_job(cls, job_name: str) -> bool:
            return job_name == JobNames.DOCS_CHECK

        @classmethod
        def is_srcs_job(cls, job_name: str) -> bool:
            return not cls.is_docs_job(job_name)

        @classmethod
        def get_type_by_name(cls, job_name: str) -> "CiCache.JobType":
            res = cls.SRCS
            if cls.is_docs_job(job_name):
                res = cls.DOCS
            elif cls.is_srcs_job(job_name):
                res = cls.SRCS
            else:
                assert False
            return res

    def __init__(
        self,
        s3: S3Helper,
        job_digests: Dict[str, str],
    ):
        self.s3 = s3
        self.job_digests = job_digests
        self.cache_s3_paths = {
            job_type: f"{self._S3_CACHE_PREFIX}/{job_type.value}-{self._get_digest_for_job_type(self.job_digests, job_type)}/"
            for job_type in self.JobType
        }
        self.s3_record_prefixes = {
            record_type: record_type.value for record_type in self.RecordType
        }
        self.records: Dict["CiCache.RecordType", Dict[str, "CiCache.Record"]] = {
            record_type: {} for record_type in self.RecordType
        }

        self.cache_updated = False
        self.cache_data_fetched = True
        if not self._LOCAL_CACHE_PATH.exists():
            self._LOCAL_CACHE_PATH.mkdir(parents=True, exist_ok=True)

    def _get_digest_for_job_type(
        self, job_digests: Dict[str, str], job_type: JobType
    ) -> str:
        if job_type == self.JobType.DOCS:
            res = job_digests[JobNames.DOCS_CHECK]
        elif job_type == self.JobType.SRCS:
            # any build type job has the same digest - pick up Build.PACKAGE_RELEASE or Build.PACKAGE_ASAN as a failover
            # Build.PACKAGE_RELEASE may not exist in the list if we have reduced CI pipeline
            if Build.PACKAGE_RELEASE in job_digests:
                res = job_digests[Build.PACKAGE_RELEASE]
            elif Build.PACKAGE_ASAN in job_digests:
                # failover, if failover does not work - fix it!
                res = job_digests[Build.PACKAGE_ASAN]
            else:
                assert False, "BUG, no build job in digest' list"
        else:
            assert False, "BUG, New JobType? - please update func"
        return res

    def _get_record_file_name(
        self,
        record_type: RecordType,
        job_name: str,
        batch: int,
        num_batches: int,
        release_branch: bool,
    ) -> str:
        prefix = self.s3_record_prefixes[record_type]
        prefix_extended = (
            self._DIV2.join([prefix, self._ATTRIBUTE_RELEASE])
            if release_branch
            else prefix
        )
        assert self._DIV1 not in job_name, f"Invalid job name {job_name}"
        job_name = self._DIV2.join(
            [job_name, self.job_digests[job_name], str(batch), str(num_batches)]
        )
        file_name = self._DIV1.join([prefix_extended, job_name])
        file_name += self._RECORD_FILE_EXTENSION
        return file_name

    def _get_record_s3_path(self, job_name: str) -> str:
        return self.cache_s3_paths[self.JobType.get_type_by_name(job_name)]

    def _parse_record_file_name(
        self, record_type: RecordType, file_name: str
    ) -> Optional["CiCache.Record"]:
        # validate filename
        if (
            not file_name.endswith(self._RECORD_FILE_EXTENSION)
            or not len(file_name.split(self._DIV1)) == 2
        ):
            print("ERROR: wrong file name format")
            return None

        file_name = file_name.removesuffix(self._RECORD_FILE_EXTENSION)
        release_branch = False

        prefix_extended, job_suffix = file_name.split(self._DIV1)
        record_type_and_attribute = prefix_extended.split(self._DIV2)

        # validate filename prefix
        failure = False
        if not 0 < len(record_type_and_attribute) <= 2:
            print("ERROR: wrong file name prefix")
            failure = True
        if (
            len(record_type_and_attribute) > 1
            and record_type_and_attribute[1] != self._ATTRIBUTE_RELEASE
        ):
            print("ERROR: wrong record attribute")
            failure = True
        if record_type_and_attribute[0] != self.s3_record_prefixes[record_type]:
            print("ERROR: wrong record type")
            failure = True
        if failure:
            return None

        if (
            len(record_type_and_attribute) > 1
            and record_type_and_attribute[1] == self._ATTRIBUTE_RELEASE
        ):
            release_branch = True

        job_properties = job_suffix.split(self._DIV2)
        job_name, job_digest, batch, num_batches = (
            self._DIV2.join(job_properties[:-3]),
            job_properties[-3],
            int(job_properties[-2]),
            int(job_properties[-1]),
        )

        if not is_hex(job_digest):
            print("ERROR: wrong record job digest")
            return None

        record = self.Record(
            record_type,
            job_name,
            job_digest,
            batch,
            num_batches,
            release_branch,
            file="",
        )
        return record

    def print_status(self):
        for record_type in self.RecordType:
            GHActions.print_in_group(
                f"Cache records: [{record_type}]", list(self.records[record_type])
            )
        return self

    def update(self):
        """
        Pulls cache records from s3. Only records name w/o content.
        """
        for record_type in self.RecordType:
            prefix = self.s3_record_prefixes[record_type]
            cache_list = self.records[record_type]
            for job_type in self.JobType:
                path = self.cache_s3_paths[job_type]
                records = self.s3.list_prefix(f"{path}{prefix}", S3_BUILDS_BUCKET)
                records = [record.split("/")[-1] for record in records]
                for file in records:
                    record = self._parse_record_file_name(
                        record_type=record_type, file_name=file
                    )
                    if not record:
                        print(f"ERROR: failed to parse cache record [{file}]")
                        continue
                    if (
                        record.job_name not in self.job_digests
                        or self.job_digests[record.job_name] != record.job_digest
                    ):
                        # skip records we are not interested in
                        continue

                    if record.to_str_key() not in cache_list:
                        cache_list[record.to_str_key()] = record
                        self.cache_data_fetched = False
                    elif (
                        not cache_list[record.to_str_key()].release_branch
                        and record.release_branch
                    ):
                        # replace a non-release record with a release one
                        cache_list[record.to_str_key()] = record
                        self.cache_data_fetched = False

        self.cache_updated = True
        return self

    def fetch_records_data(self):
        """
        Pulls CommitStatusData for all cached jobs from s3
        """
        if not self.cache_updated:
            self.update()

        if self.cache_data_fetched:
            # there are no records without fetched data - no need to fetch
            return self

        # clean up
        for file in self._LOCAL_CACHE_PATH.glob("*.ci"):
            file.unlink()

        # download all record files
        for job_type in self.JobType:
            path = self.cache_s3_paths[job_type]
            for record_type in self.RecordType:
                prefix = self.s3_record_prefixes[record_type]
                _ = self.s3.download_files(
                    bucket=S3_BUILDS_BUCKET,
                    s3_path=f"{path}{prefix}",
                    file_suffix=self._RECORD_FILE_EXTENSION,
                    local_directory=self._LOCAL_CACHE_PATH,
                )

        # validate we have files for all records and save file names meanwhile
        for record_type in self.RecordType:
            record_list = self.records[record_type]
            for _, record in record_list.items():
                record_file_name = self._get_record_file_name(
                    record_type,
                    record.job_name,
                    record.batch,
                    record.num_batches,
                    record.release_branch,
                )
                assert (
                    self._LOCAL_CACHE_PATH / record_file_name
                ).is_file(), f"BUG. Record file must be present: {self._LOCAL_CACHE_PATH / record_file_name}"
                record.file = record_file_name

        self.cache_data_fetched = True
        return self

    def exist(
        self,
        record_type: "CiCache.RecordType",
        job: str,
        batch: int,
        num_batches: int,
        release_branch: bool,
    ) -> bool:
        if not self.cache_updated:
            self.update()
        record_key = self.Record(
            record_type,
            job,
            self.job_digests[job],
            batch,
            num_batches,
            release_branch,
        ).to_str_key()
        res = record_key in self.records[record_type]
        if release_branch:
            return res and self.records[record_type][record_key].release_branch
        else:
            return res

    def push(
        self,
        record_type: "CiCache.RecordType",
        job: str,
        batches: Union[int, Sequence[int]],
        num_batches: int,
        status: Union[CommitStatusData, PendingState],
        release_branch: bool = False,
    ) -> None:
        """
        Pushes a cache record (CommitStatusData)
        @release_branch adds "release" attribute to a record
        """
        if isinstance(batches, int):
            batches = [batches]
        for batch in batches:
            record_file = self._LOCAL_CACHE_PATH / self._get_record_file_name(
                record_type, job, batch, num_batches, release_branch
            )
            record_s3_path = self._get_record_s3_path(job)
            if record_type == self.RecordType.SUCCESSFUL:
                assert isinstance(status, CommitStatusData)
                status.dump_to_file(record_file)
            elif record_type == self.RecordType.FAILED:
                assert isinstance(status, CommitStatusData)
                status.dump_to_file(record_file)
            elif record_type == self.RecordType.PENDING:
                assert isinstance(status, PendingState)
                with open(record_file, "w", encoding="utf-8") as json_file:
                    json.dump(asdict(status), json_file)
            else:
                assert False

            _ = self.s3.upload_file(
                bucket=S3_BUILDS_BUCKET,
                file_path=record_file,
                s3_path=record_s3_path + record_file.name,
            )
            record = self.Record(
                record_type,
                job,
                self.job_digests[job],
                batch,
                num_batches,
                release_branch,
                file=record_file.name,
            )
            if (
                record.release_branch
                or record.to_str_key() not in self.records[record_type]
            ):
                self.records[record_type][record.to_str_key()] = record

    def get(
        self, record_type: "CiCache.RecordType", job: str, batch: int, num_batches: int
    ) -> Optional[Union[CommitStatusData, PendingState]]:
        """
        Gets a cache record data for a job, or None if a cache miss
        """

        if not self.cache_data_fetched:
            self.fetch_records_data()

        record_key = self.Record(
            record_type,
            job,
            self.job_digests[job],
            batch,
            num_batches,
            release_branch=False,
        ).to_str_key()

        if record_key not in self.records[record_type]:
            return None

        record_file_name = self.records[record_type][record_key].file

        res = CommitStatusData.load_from_file(
            self._LOCAL_CACHE_PATH / record_file_name
        )  # type: CommitStatusData

        return res

    def delete(
        self,
        record_type: "CiCache.RecordType",
        job: str,
        batch: int,
        num_batches: int,
        release_branch: bool,
    ) -> None:
        """
        deletes record from the cache
        """
        raise NotImplementedError("Let's try make cache push-and-read-only")
        # assert (
        #     record_type == self.RecordType.PENDING
        # ), "FIXME: delete is supported for pending records only"
        # record_file_name = self._get_record_file_name(
        #     self.RecordType.PENDING,
        #     job,
        #     batch,
        #     num_batches,
        #     release_branch=release_branch,
        # )
        # record_s3_path = self._get_record_s3_path(job)
        # self.s3.delete_file_from_s3(S3_BUILDS_BUCKET, record_s3_path + record_file_name)

        # record_key = self.Record(
        #     record_type,
        #     job,
        #     self.job_digests[job],
        #     batch,
        #     num_batches,
        #     release_branch=False,
        # ).to_str_key()

        # if record_key in self.records[record_type]:
        #     del self.records[record_type][record_key]

    def is_successful(
        self, job: str, batch: int, num_batches: int, release_branch: bool
    ) -> bool:
        """
        checks if a given job have already been done successfuly
        """
        return self.exist(
            self.RecordType.SUCCESSFUL, job, batch, num_batches, release_branch
        )

    def is_failed(
        self, job: str, batch: int, num_batches: int, release_branch: bool
    ) -> bool:
        """
        checks if a given job have already been done with failure
        """
        return self.exist(
            self.RecordType.FAILED, job, batch, num_batches, release_branch
        )

    def is_pending(
        self, job: str, batch: int, num_batches: int, release_branch: bool
    ) -> bool:
        """
        check pending record in the cache for a given job
        @release_branch - checks that "release" attribute is set for a record
        """
        if self.is_successful(
            job, batch, num_batches, release_branch
        ) or self.is_failed(job, batch, num_batches, release_branch):
            return False

        return self.exist(
            self.RecordType.PENDING, job, batch, num_batches, release_branch
        )

    def push_successful(
        self,
        job: str,
        batch: int,
        num_batches: int,
        job_status: CommitStatusData,
        release_branch: bool = False,
    ) -> None:
        """
        Pushes a cache record (CommitStatusData)
        @release_branch adds "release" attribute to a record
        """
        self.push(
            self.RecordType.SUCCESSFUL,
            job,
            [batch],
            num_batches,
            job_status,
            release_branch,
        )

    def push_failed(
        self,
        job: str,
        batch: int,
        num_batches: int,
        job_status: CommitStatusData,
        release_branch: bool = False,
    ) -> None:
        """
        Pushes a cache record of type Failed (CommitStatusData)
        @release_branch adds "release" attribute to a record
        """
        self.push(
            self.RecordType.FAILED,
            job,
            [batch],
            num_batches,
            job_status,
            release_branch,
        )

    def push_pending(
        self, job: str, batches: List[int], num_batches: int, release_branch: bool
    ) -> None:
        """
        pushes pending record for a job to the cache
        """
        pending_state = PendingState(time.time(), run_url=GITHUB_RUN_URL)
        self.push(
            self.RecordType.PENDING,
            job,
            batches,
            num_batches,
            pending_state,
            release_branch,
        )

    def get_successful(
        self, job: str, batch: int, num_batches: int
    ) -> Optional[CommitStatusData]:
        """
        Gets a cache record (CommitStatusData) for a job, or None if a cache miss
        """
        res = self.get(self.RecordType.SUCCESSFUL, job, batch, num_batches)
        assert res is None or isinstance(res, CommitStatusData)
        return res

    def delete_pending(
        self, job: str, batch: int, num_batches: int, release_branch: bool
    ) -> None:
        """
        deletes pending record from the cache
        """
        self.delete(self.RecordType.PENDING, job, batch, num_batches, release_branch)

    def download_build_reports(self, file_prefix: str = "") -> List[str]:
        """
        not ideal class for this method,
        but let it be as we store build reports in CI cache directory on s3
        and CiCache knows where exactly

        @file_prefix allows to filter out reports by git head_ref
        """
        report_path = Path(REPORT_PATH)
        report_path.mkdir(exist_ok=True, parents=True)
        path = (
            self._get_record_s3_path(Build.PACKAGE_RELEASE)
            + self._CACHE_BUILD_REPORT_PREFIX
        )
        if file_prefix:
            path += "_" + file_prefix
        reports_files = self.s3.download_files(
            bucket=S3_BUILDS_BUCKET,
            s3_path=path,
            file_suffix=".json",
            local_directory=report_path,
        )
        return reports_files

    def upload_build_report(self, build_result: BuildResult) -> str:
        result_json_path = build_result.write_json(Path(TEMP_PATH))
        s3_path = (
            self._get_record_s3_path(Build.PACKAGE_RELEASE) + result_json_path.name
        )
        return self.s3.upload_file(
            bucket=S3_BUILDS_BUCKET, file_path=result_json_path, s3_path=s3_path
        )

    def await_jobs(
        self, jobs_with_params: Dict[str, Dict[str, Any]], is_release_branch: bool
    ) -> Dict[str, List[int]]:
        """
        await pending jobs to be finished
        @jobs_with_params - jobs to await. {JOB_NAME: {"batches": [BATCHES...], "num_batches": NUM_BATCHES}}
        returns successfully finished jobs: {JOB_NAME: [BATCHES...]}
        """
        if not jobs_with_params:
            return {}
        poll_interval_sec = 300
        # TIMEOUT * MAX_ROUNDS_TO_WAIT must be less than 6h (GH job timeout) with a room for rest RunConfig work
        TIMEOUT = 3000  # 50 min
        MAX_ROUNDS_TO_WAIT = 6
        MAX_JOB_NUM_TO_WAIT = 3
        await_finished: Dict[str, List[int]] = {}
        round_cnt = 0
        while (
            len(jobs_with_params) > MAX_JOB_NUM_TO_WAIT
            and round_cnt < MAX_ROUNDS_TO_WAIT
        ):
            round_cnt += 1
            GHActions.print_in_group(
                f"Wait pending jobs, round [{round_cnt}/{MAX_ROUNDS_TO_WAIT}]:",
                list(jobs_with_params),
            )
            # this is initial approach to wait pending jobs:
            # start waiting for the next TIMEOUT seconds if there are more than X(=4) jobs to wait
            # wait TIMEOUT seconds in rounds. Y(=5) is the max number of rounds
            expired_sec = 0
            start_at = int(time.time())
            while expired_sec < TIMEOUT and jobs_with_params:
                time.sleep(poll_interval_sec)
                self.update()
                jobs_with_params_copy = deepcopy(jobs_with_params)
                for job_name in jobs_with_params:
                    num_batches = jobs_with_params[job_name]["num_batches"]
                    job_config = CI_CONFIG.get_job_config(job_name)
                    for batch in jobs_with_params[job_name]["batches"]:
                        if self.is_pending(
                            job_name,
                            batch,
                            num_batches,
                            release_branch=is_release_branch
                            and job_config.required_on_release_branch,
                        ):
                            continue
                        print(
                            f"Job [{job_name}_[{batch}/{num_batches}]] is not pending anymore"
                        )

                        # some_job_ready = True
                        jobs_with_params_copy[job_name]["batches"].remove(batch)
                        if not jobs_with_params_copy[job_name]["batches"]:
                            del jobs_with_params_copy[job_name]

                        if not self.is_successful(
                            job_name,
                            batch,
                            num_batches,
                            release_branch=is_release_branch
                            and job_config.required_on_release_branch,
                        ):
                            print(
                                f"NOTE: Job [{job_name}:{batch}] finished but no success - remove from awaiting list, do not add to ready"
                            )
                            continue
                        if job_name in await_finished:
                            await_finished[job_name].append(batch)
                        else:
                            await_finished[job_name] = [batch]
                jobs_with_params = jobs_with_params_copy
                expired_sec = int(time.time()) - start_at
                print(
                    f"...awaiting continues... seconds left [{TIMEOUT - expired_sec}]"
                )
            if await_finished:
                GHActions.print_in_group(
                    f"Finished jobs, round [{round_cnt}]:",
                    [f"{job}:{batches}" for job, batches in await_finished.items()],
                )
        GHActions.print_in_group(
            "Remaining jobs:",
            [f"{job}:{params['batches']}" for job, params in jobs_with_params.items()],
        )
        return await_finished


@dataclass
class CiOptions:
    # job will be included in the run if any keyword from the list matches job name
    include_keywords: Optional[List[str]] = None
    # job will be excluded in the run if any keyword from the list matches job name
    exclude_keywords: Optional[List[str]] = None

    # list of specified preconfigured ci sets to run
    ci_sets: Optional[List[str]] = None
    # list of specified jobs to run
    ci_jobs: Optional[List[str]] = None

    # btaches to run for all multi-batch jobs
    job_batches: Optional[List[int]] = None

    do_not_test: bool = False
    no_ci_cache: bool = False
    no_merge_commit: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def create_from_run_config(run_config: Dict[str, Any]) -> "CiOptions":
        return CiOptions(**run_config["ci_options"])

    @staticmethod
    def create_from_pr_message(
        debug_message: Optional[str], update_from_api: bool
    ) -> "CiOptions":
        """
        Creates CiOptions instance based on tags found in PR body and/or commit message
        @commit_message - may be provided directly for debugging purposes, otherwise it will be retrieved from git.
        """
        res = CiOptions()
        pr_info = PRInfo()
        if (
            not pr_info.is_pr() and not debug_message
        ):  # if commit_message is provided it's test/debug scenario - do not return
            # CI options can be configured in PRs only
            # if debug_message is provided - it's a test
            return res
        message = debug_message or GitRunner(set_cwd_to_git_root=True).run(
            f"{GIT_PREFIX} log {pr_info.sha} --format=%B -n 1"
        )

        pattern = r"(#|- \[x\] +<!---)(\w+)"
        matches = [match[-1] for match in re.findall(pattern, message)]
        print(f"CI tags from commit message: [{matches}]")

        if not debug_message:  # to be skipped if debug/test
            pr_info = PRInfo(
                pr_event_from_api=update_from_api
            )  # Fetch updated PR body from GH API
            matches_pr = [match[-1] for match in re.findall(pattern, pr_info.body)]
            print(f"CI tags from PR body: [{matches_pr}]")
            matches = list(set(matches + matches_pr))

            if "do not test" in pr_info.labels:
                # do_not_test could be set in GH labels
                res.do_not_test = True

        for match in matches:
            if match.startswith("job_"):
                if not res.ci_jobs:
                    res.ci_jobs = []
                res.ci_jobs.append(match.removeprefix("job_"))
            elif match.startswith("ci_set_") and match in Labels:
                if not res.ci_sets:
                    res.ci_sets = []
                res.ci_sets.append(match)
            elif match.startswith("ci_include_"):
                if not res.include_keywords:
                    res.include_keywords = []
                res.include_keywords.append(
                    normalize_check_name(match.removeprefix("ci_include_"))
                )
            elif match.startswith("ci_exclude_"):
                if not res.exclude_keywords:
                    res.exclude_keywords = []
                res.exclude_keywords.append(
                    normalize_check_name(match.removeprefix("ci_exclude_"))
                )
            elif match == Labels.NO_CI_CACHE:
                res.no_ci_cache = True
                print("NOTE: CI Cache will be disabled")
            elif match == Labels.DO_NOT_TEST_LABEL:
                res.do_not_test = True
            elif match == Labels.NO_MERGE_COMMIT:
                res.no_merge_commit = True
                print("NOTE: Merge Commit will be disabled")
            elif match.startswith("batch_"):
                batches = []
                try:
                    batches = [
                        int(batch) for batch in match.removeprefix("batch_").split("_")
                    ]
                except Exception:
                    print(f"ERROR: failed to parse commit tag [{match}] - skip")
                if batches:
                    if not res.job_batches:
                        res.job_batches = []
                    res.job_batches += batches
                    res.job_batches = list(set(res.job_batches))
            else:
                print(
                    f"WARNING: Invalid tag in commit message or PR body [{match}] - skip"
                )

        return res

    def apply(
        self,
        jobs_to_do: List[str],
        jobs_to_skip: List[str],
        jobs_params: Dict[str, Dict[str, Any]],
    ) -> Tuple[List[str], List[str], Dict[str, Dict[str, Any]]]:
        """
        Applies specified options on CI Run Config
        Returns updated jobs_to_do, jobs_to_skip, jobs_params
        """
        jobs_to_do_requested = []  # type: List[str]

        # -1. Handle "ci_exclude_" tags if any
        if self.exclude_keywords:
            new_jobs_to_do = list(jobs_to_do)
            for job in jobs_to_do:
                found = False
                for keyword in self.exclude_keywords:
                    if keyword in normalize_check_name(job):
                        print(
                            f"Job [{job}] matches Exclude keyword [{keyword}] - remove"
                        )
                        found = True
                        break
                if found:
                    new_jobs_to_do.remove(job)
            jobs_to_do = new_jobs_to_do

        # 0. Handle "ci_include_" tags if any
        if self.include_keywords:
            for job in jobs_to_do:
                found = False
                for keyword in self.include_keywords:
                    if keyword in normalize_check_name(job):
                        print(f"Job [{job}] matches Include keyword [{keyword}] - add")
                        found = True
                        break
                if found:
                    job_with_parents = CI_CONFIG.get_job_with_parents(job)
                    for job in job_with_parents:
                        if job in jobs_to_do and job not in jobs_to_do_requested:
                            jobs_to_do_requested.append(job)
            assert (
                jobs_to_do_requested
            ), "Include tags are set but now job configured - Invalid tags, probably [{self.include_keywords}]"
            if JobNames.STYLE_CHECK not in jobs_to_do_requested:
                # Style check must not be omitted
                jobs_to_do_requested.append(JobNames.STYLE_CHECK)

        # FIXME: to be removed in favor of include/exclude
        # 1. Handle "ci_set_" tags if any
        if self.ci_sets:
            for tag in self.ci_sets:
                label_config = CI_CONFIG.get_label_config(tag)
                assert label_config, f"Unknonwn tag [{tag}]"
                print(
                    f"NOTE: CI Set's tag: [{tag}], add jobs: [{label_config.run_jobs}]"
                )
                jobs_to_do_requested += label_config.run_jobs

        # FIXME: to be removed in favor of include/exclude
        # 2. Handle "job_" tags if any
        if self.ci_jobs:
            for job in self.ci_jobs:
                job_with_parents = CI_CONFIG.get_job_with_parents(job)
                print(
                    f"NOTE: CI Job's tag: [#job_{job}], add jobs: [{job_with_parents}]"
                )
                # always add requested job itself, even if it could be skipped
                jobs_to_do_requested.append(job_with_parents[0])
                for parent in job_with_parents[1:]:
                    if parent in jobs_to_do and parent not in jobs_to_do_requested:
                        jobs_to_do_requested.append(parent)

        # 3. Handle "do not test"
        if self.do_not_test:
            label_config = CI_CONFIG.get_label_config(Labels.DO_NOT_TEST_LABEL)
            assert label_config
            print(
                f"NOTE: CI 'do not test' setting applied, set jobs: [{label_config.run_jobs}]"
            )
            if jobs_to_do_requested:
                print(
                    "WARNING: 'do not test' is used alongside with other CI modifying tags - 'do not test' prevails"
                )
            jobs_to_do_requested = list(label_config.run_jobs)

        if jobs_to_do_requested:
            jobs_to_do_requested = list(set(jobs_to_do_requested))
            print(
                f"NOTE: Only specific job(s) were requested by user's input: [{jobs_to_do_requested}]"
            )
            jobs_to_do = list(
                set(job for job in jobs_to_do_requested if job not in jobs_to_skip)
            )
            # if requested job does not have params in jobs_params (it happens for "run_by_label" job)
            #   we need to add params - otherwise it won't run as "batches" list will be empty
            for job in jobs_to_do:
                if job not in jobs_params:
                    num_batches = CI_CONFIG.get_job_config(job).num_batches
                    jobs_params[job] = {
                        "batches": list(range(num_batches)),
                        "num_batches": num_batches,
                    }

        # 4. Handle "batch_" tags
        if self.job_batches:
            print(
                f"NOTE: Only specific job batches were requested [{self.job_batches}]"
            )
            for job, params in jobs_params.items():
                if params["num_batches"] > 1:
                    params["batches"] = self.job_batches

        return jobs_to_do, jobs_to_skip, jobs_params


def get_check_name(check_name: str, batch: int, num_batches: int) -> str:
    res = check_name
    if num_batches > 1:
        res = f"{check_name} [{batch+1}/{num_batches}]"
    return res


def normalize_check_name(check_name: str) -> str:
    res = check_name.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_")):
        res = res.replace(*r)
    return res


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    # FIXME: consider switching to sub_parser for configure, pre, run, post actions
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Action that configures ci run. Calculates digests, checks job to be executed, generates json output",
    )
    parser.add_argument(
        "--update-gh-statuses",
        action="store_true",
        help="Action that recreate success GH statuses for jobs that finished successfully in past and will be skipped this time",
    )
    parser.add_argument(
        "--pre",
        action="store_true",
        help="Action that executes prerequesetes for the job provided in --job-name",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given job name.",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Action that executes post actions for the job provided in --job-name",
    )
    parser.add_argument(
        "--mark-success",
        action="store_true",
        help="Action that marks job provided in --job-name (with batch provided in --batch) as successful",
    )
    parser.add_argument(
        "--job-name",
        default="",
        type=str,
        help="Job name as in config",
    )
    parser.add_argument(
        "--run-command",
        default="",
        type=str,
        help="A run command to run in --run action. Will override run_command from a job config if any",
    )
    parser.add_argument(
        "--batch",
        default=-1,
        type=int,
        help="Current batch number (required for --mark-success), -1 or omit for single-batch job",
    )
    parser.add_argument(
        "--infile",
        default="",
        type=str,
        help="Input json file or json string with ci run config",
    )
    parser.add_argument(
        "--outfile",
        default="",
        type=str,
        required=False,
        help="output file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes json output pretty formatted",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        default=False,
        help="skip fetching docker data from dockerhub, used in --configure action (for debugging)",
    )
    parser.add_argument(
        "--docker-digest-or-latest",
        action="store_true",
        default=False,
        help="temporary hack to fallback to latest if image with digest as a tag is not on docker hub",
    )
    parser.add_argument(
        "--skip-jobs",
        action="store_true",
        default=False,
        help="skip fetching data about job runs, used in --configure action (for debugging and nigthly ci)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Used with --run, force the job to run, omitting the ci cache",
    )
    # FIXME: remove, not used
    parser.add_argument(
        "--rebuild-all-binaries",
        action="store_true",
        default=False,
        help="[DEPRECATED. to be removed, once no wf use it] will create run config without skipping build jobs in any case, used in --configure action (for release branches)",
    )
    parser.add_argument(
        "--commit-message",
        default="",
        help="debug option to test commit message processing",
    )
    return parser.parse_args()


# FIXME: rewrite the docker job as regular reusable_test job and move interaction with docker hub inside job script
#   that way run config will be more clean, workflow more generic and less api calls to dockerhub
def check_missing_images_on_dockerhub(
    image_name_tag: Dict[str, str], arch: Optional[str] = None
) -> Dict[str, str]:
    """
    Checks missing images on dockerhub.
    Works concurrently for all given images.
    Docker must be logged in.
    """

    def run_docker_command(
        image: str, image_digest: str, arch: Optional[str] = None
    ) -> Dict:
        """
        aux command for fetching single docker manifest
        """
        command = [
            "docker",
            "manifest",
            "inspect",
            f"{image}:{image_digest}" if not arch else f"{image}:{image_digest}-{arch}",
        ]

        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        return {
            "image": image,
            "image_digest": image_digest,
            "arch": arch,
            "stdout": process.stdout,
            "stderr": process.stderr,
            "return_code": process.returncode,
        }

    result: Dict[str, str] = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_docker_command, image, tag, arch)
            for image, tag in image_name_tag.items()
        ]

        responses = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
        for resp in responses:
            name, stdout, stderr, digest, arch = (
                resp["image"],
                resp["stdout"],
                resp["stderr"],
                resp["image_digest"],
                resp["arch"],
            )
            if stderr:
                if stderr.startswith("no such manifest"):
                    result[name] = digest
                else:
                    print(f"Error: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Error: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Error: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


def _pre_action(s3, indata, pr_info):
    CommitStatusData.cleanup()
    JobReport.cleanup()
    BuildResult.cleanup()
    ci_cache = CiCache(s3, indata["jobs_data"]["digests"])

    # for release/master branches reports must be from the same branches
    report_prefix = normalize_string(pr_info.head_ref) if pr_info.number == 0 else ""
    print(
        f"Use report prefix [{report_prefix}], pr_num [{pr_info.number}], head_ref [{pr_info.head_ref}]"
    )
    reports_files = ci_cache.download_build_reports(file_prefix=report_prefix)
    print(f"Pre action done. Report files [{reports_files}] have been downloaded")


def _mark_success_action(
    s3: S3Helper,
    indata: Dict[str, Any],
    pr_info: PRInfo,
    job: str,
    batch: int,
) -> None:
    ci_cache = CiCache(s3, indata["jobs_data"]["digests"])
    job_config = CI_CONFIG.get_job_config(job)
    num_batches = job_config.num_batches
    # if batch is not provided - set to 0
    batch = 0 if batch == -1 else batch
    assert (
        0 <= batch < num_batches
    ), f"--batch must be provided and in range [0, {num_batches}) for {job}"

    # FIXME: find generic design for propagating and handling job status (e.g. stop using statuses in GH api)
    #   now job ca be build job w/o status data, any other job that exit with 0 with or w/o status data
    if CI_CONFIG.is_build_job(job):
        # there is no CommitStatus for build jobs
        # create dummy status relying on JobReport
        # FIXME: consider creating commit status for build jobs too, to treat everything the same way
        job_report = JobReport.load() if JobReport.exist() else None
        if job_report and job_report.status == SUCCESS:
            CommitStatusData(
                SUCCESS,
                "dummy description",
                "dummy_url",
                pr_num=pr_info.number,
                sha=pr_info.sha,
            ).dump_status()

    job_status = None
    if CommitStatusData.exist():
        # normal scenario
        job_status = CommitStatusData.load_status()
    else:
        # apparently exit after rerun-helper check
        # do nothing, exit without failure
        print(f"ERROR: no status file for job [{job}]")

    if job_config.run_always or job_config.run_by_label:
        print(f"Job [{job}] runs always or by label in CI - do not cache")
    else:
        if pr_info.is_master():
            pass
            # delete method is disabled for ci_cache. need it?
            # pending enabled for master branch jobs only
            # ci_cache.delete_pending(job, batch, num_batches, release_branch=True)
        if job_status and job_status.is_ok():
            ci_cache.push_successful(
                job, batch, num_batches, job_status, pr_info.is_release_branch()
            )
            print(f"Job [{job}] is ok")
        elif job_status and not job_status.is_ok():
            ci_cache.push_failed(
                job, batch, num_batches, job_status, pr_info.is_release_branch()
            )
            print(f"Job [{job}] is failed with status [{job_status.status}]")
        else:
            job_status = CommitStatusData(
                description="dummy description", status=ERROR, report_url="dummy url"
            )
            ci_cache.push_failed(
                job, batch, num_batches, job_status, pr_info.is_release_branch()
            )
            print(f"No CommitStatusData for [{job}], push dummy failure to ci_cache")


def _print_results(result: Any, outfile: Optional[str], pretty: bool = False) -> None:
    if outfile:
        with open(outfile, "w", encoding="utf-8") as f:
            if isinstance(result, str):
                print(result, file=f)
            elif isinstance(result, dict):
                print(json.dumps(result, indent=2 if pretty else None), file=f)
            else:
                raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    else:
        if isinstance(result, str):
            print(result)
        elif isinstance(result, dict):
            print(json.dumps(result, indent=2 if pretty else None))
        else:
            raise AssertionError(f"Unexpected type for 'res': {type(result)}")


def _update_config_for_docs_only(jobs_data: dict) -> None:
    DOCS_CHECK_JOBS = [JobNames.DOCS_CHECK, JobNames.STYLE_CHECK]
    print(f"NOTE: Will keep only docs related jobs: [{DOCS_CHECK_JOBS}]")
    jobs_to_do = jobs_data.get("jobs_to_do", [])
    jobs_data["jobs_to_do"] = [job for job in jobs_to_do if job in DOCS_CHECK_JOBS]
    jobs_data["jobs_to_wait"] = {
        job: params
        for job, params in jobs_data["jobs_to_wait"].items()
        if job in DOCS_CHECK_JOBS
    }


def _configure_docker_jobs(docker_digest_or_latest: bool) -> Dict:
    print("::group::Docker images check")
    # generate docker jobs data
    docker_digester = DockerDigester()
    imagename_digest_dict = (
        docker_digester.get_all_digests()
    )  # 'image name - digest' mapping
    images_info = docker_images_helper.get_images_info()

    # FIXME: we need login as docker manifest inspect goes directly to one of the *.docker.com hosts instead of "registry-mirrors" : ["http://dockerhub-proxy.dockerhub-proxy-zone:5000"]
    #   find if it's possible to use the setting of /etc/docker/daemon.json (https://github.com/docker/cli/issues/4484#issuecomment-1688095463)
    docker_images_helper.docker_login()
    missing_multi_dict = check_missing_images_on_dockerhub(imagename_digest_dict)
    missing_multi = list(missing_multi_dict)
    missing_amd64 = []
    missing_aarch64 = []
    if not docker_digest_or_latest:
        # look for missing arm and amd images only among missing multiarch manifests @missing_multi_dict
        # to avoid extra dockerhub api calls
        missing_amd64 = list(
            check_missing_images_on_dockerhub(missing_multi_dict, "amd64")
        )
        # FIXME: WA until full arm support: skip not supported arm images
        missing_aarch64 = list(
            check_missing_images_on_dockerhub(
                {
                    im: digest
                    for im, digest in missing_multi_dict.items()
                    if not images_info[im]["only_amd64"]
                },
                "aarch64",
            )
        )
    else:
        if missing_multi:
            assert False, f"Missing images [{missing_multi}], cannot proceed"
    print("::endgroup::")

    return {
        "images": imagename_digest_dict,
        "missing_aarch64": missing_aarch64,
        "missing_amd64": missing_amd64,
        "missing_multi": missing_multi,
    }


def _configure_jobs(
    job_digester: JobDigester,
    s3: S3Helper,
    pr_info: PRInfo,
    ci_options: CiOptions,
) -> Dict:
    ## a. digest each item from the config
    job_digester = JobDigester()
    jobs_params: Dict[str, Dict] = {}
    jobs_to_do: List[str] = []
    jobs_to_skip: List[str] = []
    digests: Dict[str, str] = {}

    print("::group::Job Digests")
    for job in CI_CONFIG.job_generator(pr_info.head_ref if CI else "dummy_branch_name"):
        digest = job_digester.get_job_digest(CI_CONFIG.get_digest_config(job))
        digests[job] = digest
        print(f"    job [{job.rjust(50)}] has digest [{digest}]")
    print("::endgroup::")

    ## b. check what we need to run
    ci_cache = None
    if not ci_options.no_ci_cache and CI:
        ci_cache = CiCache(s3, digests).update()
        ci_cache.print_status()

    jobs_to_wait: Dict[str, Dict[str, Any]] = {}
    randomization_buckets = {}  # type: Dict[str, Set[str]]

    for job, digest in digests.items():
        job_config = CI_CONFIG.get_job_config(job)
        num_batches: int = job_config.num_batches
        batches_to_do: List[int] = []
        add_to_skip = False

        if job_config.pr_only and pr_info.is_release_branch():
            continue
        if job_config.release_only and not pr_info.is_release_branch():
            continue

        # fill job randomization buckets (for jobs with configured @random_bucket property))
        if job_config.random_bucket:
            if not job_config.random_bucket in randomization_buckets:
                randomization_buckets[job_config.random_bucket] = set()
            randomization_buckets[job_config.random_bucket].add(job)

        for batch in range(num_batches):  # type: ignore
            if job_config.run_by_label:
                # this job controlled by label, add to todo if its label is set in pr
                if job_config.run_by_label in pr_info.labels:
                    batches_to_do.append(batch)
            elif job_config.run_always:
                # always add to todo
                batches_to_do.append(batch)
            elif not ci_cache:
                batches_to_do.append(batch)
            elif not ci_cache.is_successful(
                job,
                batch,
                num_batches,
                release_branch=pr_info.is_release_branch()
                and job_config.required_on_release_branch,
            ):
                # ci cache is enabled and job is not in the cache - add
                batches_to_do.append(batch)

                # check if it's pending in the cache
                if ci_cache.is_pending(
                    job,
                    batch,
                    num_batches,
                    release_branch=pr_info.is_release_branch()
                    and job_config.required_on_release_branch,
                ):
                    if job in jobs_to_wait:
                        jobs_to_wait[job]["batches"].append(batch)
                    else:
                        jobs_to_wait[job] = {
                            "batches": [batch],
                            "num_batches": num_batches,
                        }
            else:
                add_to_skip = True

        if batches_to_do:
            jobs_to_do.append(job)
            jobs_params[job] = {
                "batches": batches_to_do,
                "num_batches": num_batches,
            }
        elif add_to_skip:
            # treat job as being skipped only if it's controlled by digest
            jobs_to_skip.append(job)

    if not pr_info.is_release_branch():
        # randomization bucket filtering (pick one random job from each bucket, for jobs with configured random_bucket property)
        for _, jobs in randomization_buckets.items():
            jobs_to_remove_randomization = set()
            bucket_ = list(jobs)
            random.shuffle(bucket_)
            while len(bucket_) > 1:
                random_job = bucket_.pop()
                if random_job in jobs_to_do:
                    jobs_to_remove_randomization.add(random_job)
            if jobs_to_remove_randomization:
                print(
                    f"Following jobs will be removed due to randomization bucket: [{jobs_to_remove_randomization}]"
                )
                jobs_to_do = [
                    job for job in jobs_to_do if job not in jobs_to_remove_randomization
                ]

    jobs_to_do, jobs_to_skip, jobs_params = ci_options.apply(
        jobs_to_do, jobs_to_skip, jobs_params
    )

    if pr_info.is_merge_queue():
        # FIXME: Quick support for MQ workflow which is only StyleCheck for now
        jobs_to_do = [JobNames.STYLE_CHECK]
        jobs_to_skip = []
        print(f"NOTE: This is Merge Queue CI: set jobs to do: [{jobs_to_do}]")

    return {
        "digests": digests,
        "jobs_to_do": jobs_to_do,
        "jobs_to_skip": jobs_to_skip,
        "jobs_to_wait": {
            job: params for job, params in jobs_to_wait.items() if job in jobs_to_do
        },
        "jobs_params": {
            job: params for job, params in jobs_params.items() if job in jobs_to_do
        },
    }


def _generate_ci_stage_config(jobs_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    populates GH Actions' workflow with real jobs
    "Builds_1": [{"job_name": NAME, "runner_type": RUNER_TYPE}]
    "Tests_1": [{"job_name": NAME, "runner_type": RUNER_TYPE}]
    ...
    """
    result = {}  # type: Dict[str, Any]
    stages_to_do = []
    for job in jobs_data["jobs_to_do"]:
        stage_type = CI_CONFIG.get_job_ci_stage(job)
        if stage_type == CIStages.NA:
            continue
        if stage_type not in result:
            result[stage_type] = []
            stages_to_do.append(stage_type)
        result[stage_type].append(
            {"job_name": job, "runner_type": CI_CONFIG.get_runner_type(job)}
        )
    result["stages_to_do"] = stages_to_do
    return result


def _create_gh_status(
    commit: Any, job: str, batch: int, num_batches: int, job_status: CommitStatusData
) -> None:
    print(f"Going to re-create GH status for job [{job}]")
    assert job_status.status == SUCCESS, "BUG!"
    commit.create_status(
        state=job_status.status,
        target_url=job_status.report_url,
        description=format_description(
            f"Reused from [{job_status.pr_num}-{job_status.sha[0:8]}]: "
            f"{job_status.description}"
        ),
        context=get_check_name(job, batch=batch, num_batches=num_batches),
    )


def _update_gh_statuses_action(indata: Dict, s3: S3Helper) -> None:
    if CiOptions.create_from_run_config(indata).no_ci_cache:
        print("CI cache is disabled - skip restoring commit statuses from CI cache")
        return
    job_digests = indata["jobs_data"]["digests"]
    jobs_to_skip = indata["jobs_data"]["jobs_to_skip"]
    jobs_to_do = indata["jobs_data"]["jobs_to_do"]
    ci_cache = CiCache(s3, job_digests).update().fetch_records_data().print_status()

    # create GH status
    pr_info = PRInfo()
    commit = get_commit(GitHub(get_best_robot_token(), per_page=100), pr_info.sha)

    def _concurrent_create_status(job: str, batch: int, num_batches: int) -> None:
        job_status = ci_cache.get_successful(job, batch, num_batches)
        if not job_status:
            return
        _create_gh_status(commit, job, batch, num_batches, job_status)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for job in job_digests:
            if job not in jobs_to_skip and job not in jobs_to_do:
                # no need to create status for job that are not supposed to be executed
                continue
            if CI_CONFIG.is_build_job(job):
                # no GH status for build jobs
                continue
            job_config = CI_CONFIG.get_job_config(job)
            if not job_config:
                # there might be a new job that does not exist on this branch - skip it
                continue
            for batch in range(job_config.num_batches):
                future = executor.submit(
                    _concurrent_create_status, job, batch, job_config.num_batches
                )
                futures.append(future)
        done, _ = concurrent.futures.wait(futures)
        for future in done:
            try:
                _ = future.result()
            except Exception as e:
                raise e
    print("Going to update overall CI report")
    set_status_comment(commit, pr_info)
    print("... CI report update - done")


def _fetch_commit_tokens(message: str, pr_info: PRInfo) -> List[str]:
    pattern = r"(#|- \[x\] +<!---)(\w+)"
    matches = [match[-1] for match in re.findall(pattern, message)]
    res = [
        match
        for match in matches
        if match in Labels or match.startswith("job_") or match.startswith("batch_")
    ]
    print(f"CI modifyers from commit message: [{res}]")
    res_2 = []
    if pr_info.is_pr():
        matches = [match[-1] for match in re.findall(pattern, pr_info.body)]
        res_2 = [
            match
            for match in matches
            if match in Labels or match.startswith("job_") or match.startswith("batch_")
        ]
        print(f"CI modifyers from PR body: [{res_2}]")
    return list(set(res + res_2))


def _upload_build_artifacts(
    pr_info: PRInfo,
    build_name: str,
    ci_cache: CiCache,
    job_report: JobReport,
    s3: S3Helper,
    s3_destination: str,
) -> str:
    # There are ugly artifacts for the performance test. FIXME:
    s3_performance_path = "/".join(
        (
            get_release_or_pr(pr_info, get_version_from_repo())[1],
            pr_info.sha,
            normalize_string(build_name),
            "performance.tar.zst",
        )
    )
    performance_urls = []
    assert job_report.build_dir_for_upload, "Must be set for build job"
    performance_path = Path(job_report.build_dir_for_upload) / "performance.tar.zst"
    if performance_path.exists():
        performance_urls.append(
            s3.upload_build_file_to_s3(performance_path, s3_performance_path)
        )
        print(
            "Uploaded performance.tar.zst to %s, now delete to avoid duplication",
            performance_urls[0],
        )
        performance_path.unlink()
    build_urls = (
        s3.upload_build_directory_to_s3(
            Path(job_report.build_dir_for_upload),
            s3_destination,
            keep_dirs_in_s3_path=False,
            upload_symlinks=False,
        )
        + performance_urls
    )
    print("::notice ::Build URLs: {}".format("\n".join(build_urls)))
    log_path = Path(job_report.additional_files[0])
    log_url = ""
    if log_path.exists():
        log_url = s3.upload_build_file_to_s3(
            log_path, s3_destination + "/" + log_path.name
        )
    print(f"::notice ::Log URL: {log_url}")

    # generate and upload build report
    build_result = BuildResult(
        build_name,
        log_url,
        build_urls,
        job_report.version,
        job_report.status,
        int(job_report.duration),
        GITHUB_JOB_API_URL(),
        head_ref=pr_info.head_ref,
        pr_number=pr_info.number,
    )
    report_url = ci_cache.upload_build_report(build_result)
    print(f"Report file has been uploaded to [{report_url}]")

    # Upload head master binaries
    static_bin_name = CI_CONFIG.build_config[build_name].static_binary_name
    if pr_info.is_master() and static_bin_name:
        # Full binary with debug info:
        s3_path_full = "/".join((pr_info.base_ref, static_bin_name, "clickhouse-full"))
        binary_full = Path(job_report.build_dir_for_upload) / "clickhouse"
        url_full = s3.upload_build_file_to_s3(binary_full, s3_path_full)
        print(f"::notice ::Binary static URL (with debug info): {url_full}")

        # Stripped binary without debug info:
        s3_path_compact = "/".join((pr_info.base_ref, static_bin_name, "clickhouse"))
        binary_compact = Path(job_report.build_dir_for_upload) / "clickhouse-stripped"
        url_compact = s3.upload_build_file_to_s3(binary_compact, s3_path_compact)
        print(f"::notice ::Binary static URL (compact): {url_compact}")

    return log_url


def _upload_build_profile_data(
    pr_info: PRInfo,
    build_name: str,
    job_report: JobReport,
    git_runner: GitRunner,
    ch_helper: ClickHouseHelper,
) -> None:
    ci_logs_credentials = CiLogsCredentials(Path("/dev/null"))
    if ci_logs_credentials.host:
        instance_type = get_instance_type()
        instance_id = get_instance_id()
        query = f"""INSERT INTO build_time_trace
            (
                pull_request_number,
                commit_sha,
                check_start_time,
                check_name,
                instance_type,
                instance_id,
                file,
                library,
                time,
                pid,
                tid,
                ph,
                ts,
                dur,
                cat,
                name,
                detail,
                count,
                avgMs,
                args_name
            )
            SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', *
            FROM input('
                file String,
                library String,
                time DateTime64(6),
                pid UInt32,
                tid UInt32,
                ph String,
                ts UInt64,
                dur UInt64,
                cat String,
                name String,
                detail String,
                count UInt64,
                avgMs UInt64,
                args_name String')
            FORMAT JSONCompactEachRow"""

        auth = {
            "X-ClickHouse-User": "ci",
            "X-ClickHouse-Key": ci_logs_credentials.password,
        }
        url = f"https://{ci_logs_credentials.host}/"
        profiles_dir = Path(TEMP_PATH) / "profiles_source"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        print(
            "Processing profile JSON files from %s",
            Path(REPO_COPY) / "build_docker",
        )
        git_runner(
            "./utils/prepare-time-trace/prepare-time-trace.sh "
            f"build_docker {profiles_dir.absolute()}"
        )
        profile_data_file = Path(TEMP_PATH) / "profile.json"
        with open(profile_data_file, "wb") as profile_fd:
            for profile_source in profiles_dir.iterdir():
                if profile_source.name != "binary_sizes.txt":
                    with open(profiles_dir / profile_source, "rb") as ps_fd:
                        profile_fd.write(ps_fd.read())

        print(
            "::notice ::Log Uploading profile data, path: %s, size: %s, query: %s",
            profile_data_file,
            profile_data_file.stat().st_size,
            query,
        )
        try:
            ch_helper.insert_file(url, auth, query, profile_data_file)
        except InsertException:
            logging.error("Failed to insert profile data for the build, continue")

        query = f"""INSERT INTO binary_sizes
            (
                pull_request_number,
                commit_sha,
                check_start_time,
                check_name,
                instance_type,
                instance_id,
                file,
                size
            )
            SELECT {pr_info.number}, '{pr_info.sha}', '{job_report.start_time}', '{build_name}', '{instance_type}', '{instance_id}', file, size
            FROM input('size UInt64, file String')
            SETTINGS format_regexp = '^\\s*(\\d+) (.+)$'
            FORMAT Regexp"""

        binary_sizes_file = profiles_dir / "binary_sizes.txt"

        print(
            "::notice ::Log Uploading binary sizes data, path: %s, size: %s, query: %s",
            binary_sizes_file,
            binary_sizes_file.stat().st_size,
            query,
        )
        try:
            ch_helper.insert_file(url, auth, query, binary_sizes_file)
        except InsertException:
            logging.error("Failed to insert binary_size_file for the build, continue")


def _add_build_to_version_history(
    pr_info: PRInfo,
    job_report: JobReport,
    version: str,
    docker_tag: str,
    ch_helper: ClickHouseHelper,
) -> None:
    # with some probability we will not silently break this logic
    assert pr_info.sha and pr_info.commit_html_url and pr_info.head_ref and version

    data = {
        "check_start_time": job_report.start_time,
        "pull_request_number": pr_info.number,
        "pull_request_url": pr_info.pr_html_url,
        "commit_sha": pr_info.sha,
        "commit_url": pr_info.commit_html_url,
        "version": version,
        "docker_tag": docker_tag,
        "git_ref": pr_info.head_ref,
    }

    print(f"::notice ::Log Adding record to versions history: {data}")

    ch_helper.insert_event_into(db="default", table="version_history", event=data)


def _run_test(job_name: str, run_command: str) -> int:
    assert (
        run_command or CI_CONFIG.get_job_config(job_name).run_command
    ), "Run command must be provided as input argument or be configured in job config"

    if CI_CONFIG.get_job_config(job_name).timeout:
        os.environ["KILL_TIMEOUT"] = str(CI_CONFIG.get_job_config(job_name).timeout)

    if not run_command:
        run_command = "/".join(
            (os.path.dirname(__file__), CI_CONFIG.get_job_config(job_name).run_command)
        )
        if ".py" in run_command and not run_command.startswith("python"):
            run_command = "python3 " + run_command
        print("Use run command from a job config")
    else:
        print("Use run command from the workflow")
    os.environ["CHECK_NAME"] = job_name
    print(f"Going to start run command [{run_command}]")
    process = subprocess.run(
        run_command,
        stdout=sys.stdout,
        stderr=sys.stderr,
        text=True,
        check=False,
        shell=True,
    )

    if process.returncode == 0:
        print(f"Run action done for: [{job_name}]")
        exit_code = 0
    else:
        print(
            f"Run action failed for: [{job_name}] with exit code [{process.returncode}]"
        )
        exit_code = process.returncode
    return exit_code


def _get_ext_check_name(check_name: str) -> str:
    run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
    run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    if run_by_hash_total > 1:
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        check_name_with_group = check_name
    return check_name_with_group


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if args.mark_success or args.pre or args.run:
        assert args.infile, "Run config must be provided via --infile"
        assert args.job_name, "Job name must be provided via --job-name"

    indata: Optional[Dict[str, Any]] = None
    if args.infile:
        if os.path.isfile(args.infile):
            with open(args.infile, encoding="utf-8") as jfd:
                indata = json.load(jfd)
        else:
            indata = json.loads(args.infile)
        assert indata and isinstance(indata, dict), "Invalid --infile json"

    result: Dict[str, Any] = {}
    s3 = S3Helper()
    pr_info = PRInfo()
    git_runner = GitRunner(set_cwd_to_git_root=True)

    ### CONFIGURE action: start
    if args.configure:
        ci_options = CiOptions.create_from_pr_message(
            args.commit_message or None, update_from_api=True
        )

        # tokens = _fetch_commit_tokens(message, pr_info)
        if ci_options.no_merge_commit and CI:
            git_runner.run(f"{GIT_PREFIX} checkout {pr_info.sha}")
            git_ref = git_runner.run(f"{GIT_PREFIX} rev-parse HEAD")

        docker_data = {}
        git_ref = git_runner.run(f"{GIT_PREFIX} rev-parse HEAD")

        # let's get CH version
        version = get_version_from_repo(git=Git(True)).string
        print(f"Got CH version for this commit: [{version}]")

        docker_data = (
            _configure_docker_jobs(args.docker_digest_or_latest)
            if not args.skip_docker
            else {}
        )

        job_digester = JobDigester()
        build_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config("package_release")
        )
        docs_digest = job_digester.get_job_digest(
            CI_CONFIG.get_digest_config(JobNames.DOCS_CHECK)
        )
        jobs_data = (
            _configure_jobs(
                job_digester,
                s3,
                pr_info,
                ci_options,
            )
            if not args.skip_jobs
            else {}
        )

        if not args.skip_jobs and pr_info.has_changes_in_documentation_only():
            _update_config_for_docs_only(jobs_data)

        if not args.skip_jobs:
            ci_cache = CiCache(s3, jobs_data["digests"])

            if pr_info.is_master():
                # wait for pending jobs to be finished, await_jobs is a long blocking call
                # wait pending jobs (for now only on release/master branches)
                ready_jobs_batches_dict = ci_cache.await_jobs(
                    jobs_data.get("jobs_to_wait", {}), pr_info.is_release_branch()
                )
                jobs_to_do = jobs_data["jobs_to_do"]
                jobs_to_skip = jobs_data["jobs_to_skip"]
                jobs_params = jobs_data["jobs_params"]
                for job, batches in ready_jobs_batches_dict.items():
                    if job not in jobs_params:
                        print(f"WARNING: Job [{job}] is not in the params list")
                        continue
                    for batch in batches:
                        jobs_params[job]["batches"].remove(batch)
                    if not jobs_params[job]["batches"]:
                        jobs_to_do.remove(job)
                        jobs_to_skip.append(job)
                        del jobs_params[job]

            # set planned jobs as pending in the CI cache if on the master
            if pr_info.is_master():
                for job in jobs_data["jobs_to_do"]:
                    config = CI_CONFIG.get_job_config(job)
                    if config.run_always or config.run_by_label:
                        continue
                    job_params = jobs_data["jobs_params"][job]
                    ci_cache.push_pending(
                        job,
                        job_params["batches"],
                        config.num_batches,
                        release_branch=pr_info.is_release_branch(),
                    )

            if "jobs_to_wait" in jobs_data:
                del jobs_data["jobs_to_wait"]

        # conclude results
        result["git_ref"] = git_ref
        result["version"] = version
        result["build"] = build_digest
        result["docs"] = docs_digest
        result["ci_options"] = ci_options.as_dict()
        if not args.skip_jobs:
            result["stages_data"] = _generate_ci_stage_config(jobs_data)
        result["jobs_data"] = jobs_data
        result["docker_data"] = docker_data
    ### CONFIGURE action: end

    ### PRE action: start
    elif args.pre:
        assert indata, "Run config must be provided via --infile"
        _pre_action(s3, indata, pr_info)

    ### RUN action: start
    elif args.run:
        assert indata
        ci_options = CiOptions.create_from_run_config(indata)
        check_name = args.job_name
        check_name_with_group = _get_ext_check_name(check_name)
        print(
            f"Check if rerun for name: [{check_name}], extended name [{check_name_with_group}]"
        )
        previous_status = None
        if CI_CONFIG.is_build_job(check_name):
            # this is a build job - check if build report is present
            build_result = (
                BuildResult.load_any(check_name, pr_info.number, pr_info.head_ref)
                if not ci_options.no_ci_cache
                else None
            )
            if build_result:
                if build_result.status == SUCCESS:
                    previous_status = build_result.status
                else:
                    # FIXME: Consider reusing failures for build jobs.
                    #   Just remove this if/else - that makes build job starting and failing immediately
                    print(
                        "Build report found but status is unsuccessful - will try to rerun"
                    )
                print("::group::Build Report")
                print(build_result.as_json())
                print("::endgroup::")
        else:
            # this is a test job - check if GH commit status or cache record is present
            commit = get_commit(
                GitHub(get_best_robot_token(), per_page=100), pr_info.sha
            )

            # rerun helper check
            # FIXME: remove rerun_helper check and rely on ci cache only
            if check_name not in (
                # we might want to rerun reports' jobs - disable rerun check for them
                JobNames.BUILD_CHECK,
                JobNames.BUILD_CHECK_SPECIAL,
            ):
                rerun_helper = RerunHelper(commit, check_name_with_group)
                if rerun_helper.is_already_finished_by_status():
                    status = rerun_helper.get_finished_status()
                    assert status
                    previous_status = status.state
                    print("::group::Commit Status")
                    print(status)
                    print("::endgroup::")

            # ci cache check
            if not previous_status and not ci_options.no_ci_cache:
                ci_cache = CiCache(s3, indata["jobs_data"]["digests"]).update()
                job_config = CI_CONFIG.get_job_config(check_name)
                if ci_cache.is_successful(
                    check_name,
                    args.batch,
                    job_config.num_batches,
                    job_config.required_on_release_branch,
                ):
                    job_status = ci_cache.get_successful(
                        check_name, args.batch, job_config.num_batches
                    )
                    assert job_status, "BUG"
                    _create_gh_status(
                        commit,
                        check_name,
                        args.batch,
                        job_config.num_batches,
                        job_status,
                    )
                    previous_status = job_status.status
                    GHActions.print_in_group("Commit Status Data", job_status)

        if previous_status and not args.force:
            print(
                f"Commit status or Build Report is already present - job will be skipped with status: [{previous_status}]"
            )
            if previous_status == SUCCESS:
                exit_code = 0
            else:
                exit_code = 1
        else:
            exit_code = _run_test(check_name, args.run_command)
    ### RUN action: end

    ### POST action: start
    elif args.post:
        job_report = JobReport.load() if JobReport.exist() else None
        if job_report:
            ch_helper = ClickHouseHelper()
            check_url = ""

            if CI_CONFIG.is_build_job(args.job_name):
                assert (
                    indata
                ), f"--infile with config must be provided for POST action of a build type job [{args.job_name}]"
                build_name = args.job_name
                s3_path_prefix = "/".join(
                    (
                        get_release_or_pr(pr_info, get_version_from_repo())[0],
                        pr_info.sha,
                        build_name,
                    )
                )
                log_url = _upload_build_artifacts(
                    pr_info,
                    build_name,
                    ci_cache=CiCache(s3, indata["jobs_data"]["digests"]),
                    job_report=job_report,
                    s3=s3,
                    s3_destination=s3_path_prefix,
                )
                _upload_build_profile_data(
                    pr_info, build_name, job_report, git_runner, ch_helper
                )
                check_url = log_url
            else:
                # test job
                additional_urls = []
                s3_path_prefix = "/".join(
                    (
                        get_release_or_pr(pr_info, get_version_from_repo())[0],
                        pr_info.sha,
                        normalize_string(
                            job_report.check_name or _get_ext_check_name(args.job_name)
                        ),
                    )
                )
                if job_report.build_dir_for_upload:
                    additional_urls = s3.upload_build_directory_to_s3(
                        Path(job_report.build_dir_for_upload),
                        s3_path_prefix,
                        keep_dirs_in_s3_path=False,
                        upload_symlinks=False,
                    )
                if job_report.test_results or job_report.additional_files:
                    check_url = upload_result_helper.upload_results(
                        s3,
                        pr_info.number,
                        pr_info.sha,
                        job_report.test_results,
                        job_report.additional_files,
                        job_report.check_name or _get_ext_check_name(args.job_name),
                        additional_urls=additional_urls or None,
                    )
                commit = get_commit(
                    GitHub(get_best_robot_token(), per_page=100), pr_info.sha
                )
                post_commit_status(
                    commit,
                    job_report.status,
                    check_url,
                    format_description(job_report.description),
                    job_report.check_name or _get_ext_check_name(args.job_name),
                    pr_info,
                    dump_to_file=True,
                )
                if not pr_info.is_merge_queue():
                    # in the merge queue mergeable status must be set only in FinishCheck (last job in wf)
                    update_mergeable_check(
                        commit,
                        pr_info,
                        job_report.check_name or _get_ext_check_name(args.job_name),
                    )

            print(f"Job report url: [{check_url}]")
            prepared_events = prepare_tests_results_for_clickhouse(
                pr_info,
                job_report.test_results,
                job_report.status,
                job_report.duration,
                job_report.start_time,
                check_url or "",
                job_report.check_name or _get_ext_check_name(args.job_name),
            )
            ch_helper.insert_events_into(
                db="default", table="checks", events=prepared_events
            )

            if "DockerServerImage" in args.job_name and indata is not None:
                _add_build_to_version_history(
                    pr_info,
                    job_report,
                    indata["version"],
                    indata["build"],
                    ch_helper,
                )
        else:
            # no job report
            print(f"No job report for {[args.job_name]} - do nothing")
    ### POST action: end

    ### MARK SUCCESS action: start
    elif args.mark_success:
        assert indata, "Run config must be provided via --infile"
        _mark_success_action(s3, indata, pr_info, args.job_name, args.batch)

    ### UPDATE GH STATUSES action: start
    elif args.update_gh_statuses:
        assert indata, "Run config must be provided via --infile"
        _update_gh_statuses_action(indata=indata, s3=s3)

    ### print results
    _print_results(result, args.outfile, args.pretty)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
