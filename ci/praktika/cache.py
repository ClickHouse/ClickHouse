import dataclasses
import json
from pathlib import Path

from praktika import Artifact, Job, Workflow
from praktika._environment import _Environment
from praktika.digest import Digest
from praktika.s3 import S3
from praktika.settings import Settings
from praktika.utils import Utils


class Cache:
    @dataclasses.dataclass
    class CacheRecord:
        class Type:
            SUCCESS = "success"

        type: str
        sha: str
        pr_number: int
        branch: str

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

    def __init__(self):
        self.digest = Digest()
        self.success = {}  # type Dict[str, Any]

    @classmethod
    def push_success_record(cls, job_name, job_digest, sha):
        type_ = Cache.CacheRecord.Type.SUCCESS
        record = Cache.CacheRecord(
            type=type_,
            sha=sha,
            pr_number=_Environment.get().PR_NUMBER,
            branch=_Environment.get().BRANCH,
        )
        assert (
            Settings.CACHE_S3_PATH
        ), f"Setting CACHE_S3_PATH must be defined with enabled CI Cache"
        record_path = f"{Settings.CACHE_S3_PATH}/v{Settings.CACHE_VERSION}/{Utils.normalize_string(job_name)}/{job_digest}"
        record_file = Path(Settings.TEMP_DIR) / type_
        record.dump(record_file)
        S3.copy_file_to_s3(s3_path=record_path, local_path=record_file)
        record_file.unlink()

    def fetch_success(self, job_name, job_digest):
        type_ = Cache.CacheRecord.Type.SUCCESS
        assert (
            Settings.CACHE_S3_PATH
        ), f"Setting CACHE_S3_PATH must be defined with enabled CI Cache"
        record_path = f"{Settings.CACHE_S3_PATH}/v{Settings.CACHE_VERSION}/{Utils.normalize_string(job_name)}/{job_digest}/{type_}"
        record_file_local_dir = (
            f"{Settings.CACHE_LOCAL_PATH}/{Utils.normalize_string(job_name)}/"
        )
        Path(record_file_local_dir).mkdir(parents=True, exist_ok=True)

        if S3.head_object(record_path):
            res = S3.copy_file_from_s3(
                s3_path=record_path, local_path=record_file_local_dir
            )
        else:
            res = None

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
                job_requirements=Job.Requirements(
                    python_requirements_txt="./ci/requirements.txt"
                ),
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
                job_requirements=Job.Requirements(
                    python_requirements_txt="./ci/requirements.txt"
                ),
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
