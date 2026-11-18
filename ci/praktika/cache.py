import dataclasses
import json
import tempfile
from pathlib import Path

from . import Artifact, Job, Workflow
from ._environment import _Environment
from .digest import Digest
from .s3 import S3
from .settings import Settings
from .utils import Utils


class Cache:
    # Workflow kv data key with per-job digest salts, see `CacheRunnerHooks.configure`.
    DIGEST_SALTS_KV_KEY = "job_digest_salts"

    @dataclasses.dataclass
    class CacheRecord:
        class Type:
            SUCCESS = "success"

        type: str
        sha: str
        pr_number: int
        branch: str
        workflow: str = ""
        # The workflow event that produced this record (Workflow.Event.*). It is
        # the trust signal for reuse: a pull_request record is untrusted, so only
        # pull_request workflows reuse it (see CacheRunnerHooks.configure).
        event: str = ""

        def dump(self, path):
            with open(path, "w", encoding="utf8") as f:
                json.dump(dataclasses.asdict(self), f)

        @classmethod
        def from_fs(cls, path):
            with open(path, "r", encoding="utf8") as f:
                return Cache.CacheRecord(**json.load(f))

        @classmethod
        def from_dict(cls, obj):
            return Cache.CacheRecord(**obj)

    @classmethod
    def artifact_dir(cls, job_name):
        """The name of the S3 directory with the artifacts of `job_name` under a run's prefix.

        A salted job (see `CacheRunnerHooks.configure`) produces a different variant of its
        artifacts than the regular job with the same name at the same commit, so it stores
        them in its own directory. Otherwise a rerun of the job at the same commit with the
        other variant overwrites them, and a cache record of the first variant, which points
        to the run's prefix, restores the artifacts of the second one. The cache record of a
        salted job has a salted digest, so a run that reuses it has the same salt and resolves
        the same directory."""
        salts = _Environment.get().JOB_KV_DATA.get(cls.DIGEST_SALTS_KV_KEY) or {}
        name = Utils.normalize_string(job_name)
        if job_name in salts:
            name += f"_{Utils.normalize_string(salts[job_name])}"
        return name

    # Object with the run's digest salts under the run's S3 prefix, see `store_digest_salts`.
    DIGEST_SALTS_FILE_NAME = "job_digest_salts.json"

    @classmethod
    def _digest_salts_s3_path(cls):
        return f"{Settings.S3_ARTIFACT_BUCKET}/{_Environment.get().get_s3_prefix()}/{cls.DIGEST_SALTS_FILE_NAME}"

    @classmethod
    def store_digest_salts(cls, salts):
        """Publish the digest salts of a run next to its artifacts.

        A local job run with `--pr`/`--sha` (see `Runner.generate_local_run_environment`) does
        not have the kv data of the run that produced the artifacts, and the PR labels at the
        time of the local run do not tell which variant was built at that commit. It restores
        the salts from this object to find the artifact directories (see `artifact_dir`).
        Written even when empty, so that a rerun at the same commit without salts replaces
        the salts of an earlier run."""
        local_path = f"{Settings.TEMP_DIR}/{cls.DIGEST_SALTS_FILE_NAME}"
        with open(local_path, "w", encoding="utf8") as f:
            json.dump(salts, f)
        S3.copy_file_to_s3(
            s3_path=cls._digest_salts_s3_path(),
            local_path=local_path,
            tags={"retention": "default"},
        )

    @classmethod
    def load_digest_salts(cls):
        """The digest salts published by `store_digest_salts` for the run at the current
        S3 prefix, None if the run did not publish them."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            if not S3.copy_file_from_s3(
                s3_path=cls._digest_salts_s3_path(), local_path=tmp_dir, no_strict=True
            ):
                return None
            with open(
                Path(tmp_dir) / cls.DIGEST_SALTS_FILE_NAME, "r", encoding="utf8"
            ) as f:
                salts = json.load(f)
        assert isinstance(salts, dict), f"Invalid digest salts [{salts}]"
        return salts

    def __init__(self):
        self.digest = Digest()
        self.success = {}  # type Dict[str, Any]

    @classmethod
    def push_success_record(
        cls, job_name, job_digest, sha, workflow_name, if_not_exist
    ):
        type_ = Cache.CacheRecord.Type.SUCCESS
        record = Cache.CacheRecord(
            type=type_,
            sha=sha,
            pr_number=_Environment.get().PR_NUMBER,
            branch=_Environment.get().BRANCH,
            workflow=workflow_name,
            event=_Environment.get().EVENT_TYPE,
        )
        assert (
            Settings.CACHE_S3_PATH
        ), "Setting CACHE_S3_PATH must be defined with enabled CI Cache"
        record_path = f"{Settings.CACHE_S3_PATH}/v{Settings.CACHE_VERSION}/{Utils.normalize_string(job_name)}/{job_digest}/{type_}"
        record_file = Path(Settings.TEMP_DIR) / type_
        record.dump(record_file)
        S3.put(
            s3_path=record_path,
            local_path=record_file,
            if_none_matched=if_not_exist,
            no_strict=True,
        )
        record_file.unlink()

    def fetch_success(self, job_name, job_digest):
        type_ = Cache.CacheRecord.Type.SUCCESS
        assert (
            Settings.CACHE_S3_PATH
        ), "Setting CACHE_S3_PATH must be defined with enabled CI Cache"
        record_path = f"{Settings.CACHE_S3_PATH}/v{Settings.CACHE_VERSION}/{Utils.normalize_string(job_name)}/{job_digest}/{type_}"
        record_file_local_dir = (
            f"{Settings.CACHE_LOCAL_PATH}/{Utils.normalize_string(job_name)}/"
        )
        Path(record_file_local_dir).mkdir(parents=True, exist_ok=True)

        # _skip_download_counter=True to avoid races for multithreaded downloads
        res = S3.copy_file_from_s3(
            s3_path=record_path,
            local_path=record_file_local_dir,
            _skip_download_counter=True,
            no_strict=True,
        )

        if res:
            print(f"Cache record found, job [{job_name}], digest [{job_digest}]")
            self.success[job_name] = True
            return Cache.CacheRecord.from_fs(Path(record_file_local_dir) / type_)
        return None


if __name__ == "__main__":
    # test
    c = Cache()
    workflow = Workflow.Config(
        name="TEST",
        event=Workflow.Event.PULL_REQUEST,
        jobs=[
            Job.Config(
                name="JobA",
                runs_on=["some"],
                command="python -m unittest ./ci/tests/example_1/test_example_produce_artifact.py",
                provides=["greet"],
                digest_config=Job.CacheDigestConfig(
                    # example: use glob to include files
                    include_paths=["./ci/tests/example_1/test_example_consume*.py"],
                ),
            ),
            Job.Config(
                name="JobB",
                runs_on=["some"],
                command="python -m unittest ./ci/tests/example_1/test_example_consume_artifact.py",
                requires=["greet"],
                digest_config=Job.CacheDigestConfig(
                    # example: use dir to include files recursively
                    include_paths=["./ci/tests/example_1"],
                    # example: use glob to exclude files from digest
                    exclude_paths=[
                        "./ci/tests/example_1/test_example_consume*",
                        "./**/*.pyc",
                    ],
                ),
            ),
        ],
        artifacts=[Artifact.Config(type="s3", name="greet", path="hello")],
        enable_cache=True,
    )
    for job in workflow.jobs:
        print(c.digest.calc_job_digest(job))
