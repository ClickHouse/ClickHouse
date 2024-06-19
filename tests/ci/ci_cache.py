import json
import time
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Any, Union, Sequence, List, Set

from ci_config import CI

from ci_utils import is_hex, GHActions
from commit_status_helper import CommitStatusData
from env_helper import (
    TEMP_PATH,
    CI_CONFIG_PATH,
    S3_BUILDS_BUCKET,
    GITHUB_RUN_URL,
    REPORT_PATH,
)
from report import BuildResult
from s3_helper import S3Helper
from digest_helper import JobDigester


@dataclass
class PendingState:
    updated_at: float
    run_url: str


class CiCache:
    """
    CI cache is a bunch of records. Record is a file stored under special location on s3.
    The file name has a format:

        <RECORD_TYPE>_[<ATTRIBUTES>]--<JOB_NAME>_<JOB_DIGEST>_<BATCH>_<NUM_BATCHES>.ci

    RECORD_TYPE:
        SUCCESSFUL - for successful jobs
        PENDING - for pending jobs

    ATTRIBUTES:
        release - for jobs being executed on the release branch including master branch (not a PR branch)
    """

    _REQUIRED_DIGESTS = [CI.JobNames.DOCS_CHECK, CI.BuildNames.PACKAGE_RELEASE]
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
            return job_name == CI.JobNames.DOCS_CHECK

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
        cache_enabled: bool = True,
    ):
        self.enabled = cache_enabled
        self.jobs_to_skip = []  # type: List[str]
        self.jobs_to_wait = {}  # type: Dict[str, CI.JobConfig]
        self.jobs_to_do = {}  # type: Dict[str, CI.JobConfig]
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

        self.updated = False
        self.cache_data_fetched = True
        if not self._LOCAL_CACHE_PATH.exists():
            self._LOCAL_CACHE_PATH.mkdir(parents=True, exist_ok=True)

    @classmethod
    def calc_digests_and_create(
        cls,
        s3: S3Helper,
        job_configs: Dict[str, CI.JobConfig],
        cache_enabled: bool = True,
        dry_run: bool = False,
    ) -> "CiCache":
        job_digester = JobDigester(dry_run=dry_run)
        digests = {}

        print("::group::Job Digests")
        for job, job_config in job_configs.items():
            digest = job_digester.get_job_digest(job_config.digest)
            digests[job] = digest
            print(f"    job [{job.rjust(50)}] has digest [{digest}]")

        for job in cls._REQUIRED_DIGESTS:
            if job not in job_configs:
                digest = job_digester.get_job_digest(CI.get_job_config(job).digest)
                digests[job] = digest
                print(
                    f"    job [{job.rjust(50)}] required for CI Cache has digest [{digest}]"
                )
        print("::endgroup::")
        return CiCache(s3, digests, cache_enabled=cache_enabled)

    def _get_digest_for_job_type(
        self, job_digests: Dict[str, str], job_type: JobType
    ) -> str:
        if job_type == self.JobType.DOCS:
            res = job_digests[CI.JobNames.DOCS_CHECK]
        elif job_type == self.JobType.SRCS:
            if CI.BuildNames.PACKAGE_RELEASE in job_digests:
                res = job_digests[CI.BuildNames.PACKAGE_RELEASE]
            else:
                assert False, "BUG, no build job in digest' list"
        else:
            assert False, "BUG, New JobType? - please update the function"
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
        print(f"Cache enabled: [{self.enabled}]")
        for record_type in self.RecordType:
            GHActions.print_in_group(
                f"Cache records: [{record_type}]", list(self.records[record_type])
            )
        GHActions.print_in_group(
            "Jobs to do:",
            list(self.jobs_to_do.items()),
        )
        GHActions.print_in_group("Jobs to skip:", self.jobs_to_skip)
        GHActions.print_in_group(
            "Jobs to wait:",
            list(self.jobs_to_wait.items()),
        )
        return self

    @staticmethod
    def dump_run_config(indata: Dict[str, Any]) -> None:
        assert indata
        assert CI_CONFIG_PATH
        with open(CI_CONFIG_PATH, "w", encoding="utf-8") as json_file:
            json.dump(indata, json_file, indent=2)

    def update(self):
        """
        Pulls cache records from s3. Only records name w/o content.
        """
        if not self.enabled:
            return self
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

        self.updated = True
        return self

    def fetch_records_data(self):
        """
        Pulls CommitStatusData for all cached jobs from s3
        """
        if not self.updated:
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
        if not self.updated:
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
        checks if a given job have already been done successfully
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

    def push_pending_all(self, release_branch: bool) -> None:
        """
        pushes pending records for all jobs that supposed to be run
        """
        for job, job_config in self.jobs_to_do.items():
            if job_config.run_always:
                continue
            pending_state = PendingState(time.time(), run_url=GITHUB_RUN_URL)
            assert job_config.batches
            self.push(
                self.RecordType.PENDING,
                job,
                job_config.batches,
                job_config.num_batches,
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
        not an ideal class for this method,
        but let it be as we store build reports in CI cache directory on s3
        and CiCache knows where exactly

        @file_prefix allows filtering out reports by git head_ref
        """
        report_path = Path(REPORT_PATH)
        report_path.mkdir(exist_ok=True, parents=True)
        path = (
            self._get_record_s3_path(CI.BuildNames.PACKAGE_RELEASE)
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
            self._get_record_s3_path(CI.BuildNames.PACKAGE_RELEASE)
            + result_json_path.name
        )
        return self.s3.upload_file(
            bucket=S3_BUILDS_BUCKET, file_path=result_json_path, s3_path=s3_path
        )

    def await_pending_jobs(self, is_release: bool, dry_run: bool = False) -> None:
        """
        await pending jobs to be finished
        @jobs_with_params - jobs to await. {JOB_NAME: {"batches": [BATCHES...], "num_batches": NUM_BATCHES}}
        returns successfully finished jobs: {JOB_NAME: [BATCHES...]}
        """
        if not self.jobs_to_wait:
            print("CI cache: no pending jobs to wait - continue")
            return

        poll_interval_sec = 300
        # TIMEOUT * MAX_ROUNDS_TO_WAIT must be less than 6h (GH job timeout) with a room for rest RunConfig work
        TIMEOUT = 3000  # 50 min
        MAX_ROUNDS_TO_WAIT = 6
        MAX_JOB_NUM_TO_WAIT = 3
        round_cnt = 0

        # FIXME: temporary experiment: lets enable await for PR' workflows but for a shorter time
        if not is_release:
            MAX_ROUNDS_TO_WAIT = 3

        while (
            len(self.jobs_to_wait) > MAX_JOB_NUM_TO_WAIT
            and round_cnt < MAX_ROUNDS_TO_WAIT
        ):
            round_cnt += 1
            GHActions.print_in_group(
                f"Wait pending jobs, round [{round_cnt}/{MAX_ROUNDS_TO_WAIT}]:",
                list(self.jobs_to_wait),
            )
            # this is an initial approach to wait pending jobs:
            # start waiting for the next TIMEOUT seconds if there are more than X(=4) jobs to wait
            # wait TIMEOUT seconds in rounds. Y(=5) is the max number of rounds
            expired_sec = 0
            start_at = int(time.time())
            while expired_sec < TIMEOUT and self.jobs_to_wait:
                await_finished: Set[str] = set()
                if not dry_run:
                    time.sleep(poll_interval_sec)
                self.update()
                for job_name, job_config in self.jobs_to_wait.items():
                    num_batches = job_config.num_batches
                    job_config = CI.get_job_config(job_name)
                    assert job_config.pending_batches
                    assert job_config.batches
                    pending_batches = list(job_config.pending_batches)
                    for batch in pending_batches:
                        if self.is_pending(
                            job_name,
                            batch,
                            num_batches,
                            release_branch=is_release
                            and job_config.required_on_release_branch,
                        ):
                            continue
                        if self.is_successful(
                            job_name,
                            batch,
                            num_batches,
                            release_branch=is_release
                            and job_config.required_on_release_branch,
                        ):
                            print(
                                f"Job [{job_name}_[{batch}/{num_batches}]] is not pending anymore"
                            )
                            job_config.batches.remove(batch)
                        else:
                            print(
                                f"NOTE: Job [{job_name}:{batch}] finished failed - do not add to ready"
                            )
                        job_config.pending_batches.remove(batch)

                        if not job_config.pending_batches:
                            await_finished.add(job_name)

                for job in await_finished:
                    self.jobs_to_skip.append(job)
                    del self.jobs_to_wait[job]
                    del self.jobs_to_do[job]

                if not dry_run:
                    expired_sec = int(time.time()) - start_at
                    print(
                        f"...awaiting continues... seconds left [{TIMEOUT - expired_sec}]"
                    )
                else:
                    # make up for 2 iterations in dry_run
                    expired_sec += int(TIMEOUT / 2) + 1

        GHActions.print_in_group(
            "Remaining jobs:",
            [list(self.jobs_to_wait)],
        )

    def apply(
        self, job_configs: Dict[str, CI.JobConfig], is_release: bool
    ) -> "CiCache":
        if not self.enabled:
            self.jobs_to_do = job_configs
            return self

        if not self.updated:
            self.update()

        for job, job_config in job_configs.items():
            assert (
                job_config.batches
            ), "Batches must be generated. check ci_settings.apply()"

            if job_config.run_always:
                self.jobs_to_do[job] = job_config
                continue

            ready_batches = []
            for batch in job_config.batches:
                if self.is_successful(
                    job,
                    batch,
                    job_config.num_batches,
                    release_branch=is_release and job_config.required_on_release_branch,
                ):
                    ready_batches.append(batch)
                elif self.is_pending(
                    job,
                    batch,
                    job_config.num_batches,
                    release_branch=is_release and job_config.required_on_release_branch,
                ):
                    if job_config.pending_batches is None:
                        job_config.pending_batches = []
                    job_config.pending_batches.append(batch)

            if ready_batches == job_config.batches:
                self.jobs_to_skip.append(job)
            else:
                for batch in ready_batches:
                    job_config.batches.remove(batch)
                self.jobs_to_do[job] = job_config
            if job_config.pending_batches:
                self.jobs_to_wait[job] = job_config

        return self
