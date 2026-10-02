"""Regression tests for the download side of required S3 artifacts.

A job whose non-optional S3 artifact was never uploaded used to be dispatched
anyway, download nothing, and die 1-3 minutes later at whatever first touched
the missing file (`dpkg: error: cannot access archive 'package_folder/*.deb'`),
reported as the pull request's own workload bug. `Runner._pre_run` now checks
the result of every download and raises for a missing non-optional artifact.

The real `S3.copy_file_from_s3` is driven against a fake boto3 client, so the
tests pin the backend's contract the check relies on: `False` for an absent key
and for a glob matching nothing, an exception for any other failure.
"""

import dataclasses
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from botocore.exceptions import ClientError

from ci.praktika import Job, Workflow
from ci.praktika._environment import _Environment
from ci.praktika.artifact import Artifact
from ci.praktika.cidb import CIDB
from ci.praktika.result import Result
from ci.praktika.runner import Runner
from ci.praktika.s3 import S3
from ci.praktika.secret import Secret
from ci.praktika.settings import Settings
from ci.praktika.utils import Utils

_GLOB_PATH = "./ci/tmp/*.deb"
_EXACT_PATH = "./ci/tmp/build/programs/self-extracting/clickhouse"
_CIDB_CONNECTION_SECRET = "CI_DB_CONNECTION"


def _client_error(code, operation):
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class _FakeS3Client:
    """The subset of the boto3 S3 client used by `S3.copy_file_from_s3`.

    Objects are stored by file name and served under whatever prefix is asked
    for, so the tests do not depend on how the artifact prefix is computed.
    """

    def __init__(self, names=(), download_error=None, list_error=None):
        self.names = list(names)
        self.download_error = download_error
        self.list_error = list_error

    def download_file(self, bucket, key, dest):
        if self.download_error is not None:
            raise self.download_error
        if Path(key).name not in self.names:
            raise _client_error("404", "HeadObject")
        Path(dest).write_text("payload")

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        client = self

        class _Paginator:
            def paginate(self, Bucket, Prefix):
                if client.list_error is not None:
                    raise client.list_error
                yield {"Contents": [{"Key": Prefix + name} for name in client.names]}

        return _Paginator()


def _environment(**overrides):
    """A minimal `_Environment`; only the overridden fields matter."""
    fields = {
        f.name: ""
        for f in dataclasses.fields(_Environment)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    }
    return _Environment(**{**fields, "PR_NUMBER": 1, **overrides})


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Drive Runner._pre_run against a fake boto3 client and a private INPUT_DIR.

    cwd is moved off the repo so `_pre_run`'s `git status --short` reports
    nothing.
    """
    monkeypatch.setattr(Settings, "S3_ARTIFACT_BUCKET", "bucket/artifacts", raising=False)
    monkeypatch.delenv("PRAKTIKA_LOCAL_RUN", raising=False)
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "ci_tmp"
    root.mkdir()
    monkeypatch.setattr(Settings, "TEMP_DIR", str(root), raising=False)
    monkeypatch.setattr(Settings, "INPUT_DIR", str(root), raising=False)
    # `_Environment.file_name_static` reads TEMP_DIR at call time, so the dump
    # has to follow the relocation. `LOCAL_RUN` stays off: it would redirect
    # `S3` to its local-filesystem backend instead of boto3.
    _environment(
        WORKFLOW_NAME="PR",
        JOB_NAME="Stress test (x)",
        REPOSITORY="o/r",
        SHA="deadbeef",
        EVENT_TYPE="pull_request",
        LOCAL_RUN=False,
    ).dump()

    def run(artifact, client=None, requires_job=False):
        """Run _pre_run for one artifact with `client` serving S3."""
        client = client or _FakeS3Client()
        monkeypatch.setattr(S3, "_get_boto3_client", classmethod(lambda cls: client))
        provider = Job.Config(
            name=artifact._provided_by,
            runs_on=["dummy"],
            command="true",
            provides=[artifact.name],
        )
        job = Job.Config(
            name="Stress test (x)",
            runs_on=["dummy"],
            command="true",
            requires=[provider.name if requires_job else artifact.name],
        )
        workflow = Workflow.Config(
            name="PR",
            event="pull_request",
            jobs=[provider, job],
            artifacts=[artifact],
            enable_cache=False,
            enable_report=False,
            enable_cidb=False,
        )
        return Runner()._pre_run(workflow, job, local_job_run=True)

    run.dir = root
    return run


def _warning_lines(capsys):
    """The captured WARNING lines only: `_pre_run` also prints the whole
    `Artifact.Config`, so a substring search over all output passes with no
    warning at all. Reading also drains the buffer."""
    return [
        line
        for line in capsys.readouterr().out.splitlines()
        if line.startswith("WARNING: optional artifact")
    ]


def _artifact(path=_GLOB_PATH, optional=False):
    """`path` may be a list, which is how ARM_FUZZERS (3 paths) is declared."""
    return Artifact.Config(
        name="DEB_ARM_MSAN",
        type=Artifact.Type.S3,
        path=path,
        optional=optional,
        _provided_by="Build (arm_msan)",
    )


@pytest.mark.parametrize("path", [_GLOB_PATH, _EXACT_PATH], ids=["glob", "exact-key"])
def test_missing_required_artifact_raises_naming_it_and_its_provider(harness, path):
    """The production shape: the provider was cancelled and uploaded nothing.

    A leftover of the same name must not pass for it - INPUT_DIR is shared by
    every artifact of the job and retained across local runs.
    """
    (harness.dir / "pkg_1.deb").write_text("stale")
    (harness.dir / "clickhouse").write_text("stale")
    with pytest.raises(FileNotFoundError) as ex:
        harness(_artifact(path=path))
    # The message must name what is missing and who owed it, or the red check
    # stays unattributable - which is the whole point of the fix.
    assert "DEB_ARM_MSAN" in str(ex.value)
    assert "Build (arm_msan)" in str(ex.value)
    assert "bucket/artifacts" in str(ex.value)


@pytest.mark.parametrize(
    "path, name", [(_GLOB_PATH, "pkg_1.deb"), (_EXACT_PATH, "clickhouse")],
    ids=["glob", "exact-key"],
)
def test_present_artifact_reaches_input_dir(harness, path, name):
    assert harness(_artifact(path=path), _FakeS3Client(names=[name])) == 0
    assert (harness.dir / name).read_text() == "payload"


def test_a_missing_artifact_is_skipped_with_a_warning_only_when_optional(
    harness, capsys
):
    """The optional policy on both the exact-key and the glob path."""
    with pytest.raises(FileNotFoundError):
        harness(_artifact(path=_EXACT_PATH))
    assert _warning_lines(capsys) == []

    assert harness(_artifact(path=_EXACT_PATH, optional=True)) == 0
    assert _warning_lines(capsys) == [
        f"WARNING: optional artifact [DEB_ARM_MSAN:{_EXACT_PATH}] is missing - skipping"
    ]

    # LLVM coverage requires 21 optional .profdata globs that may be absent.
    assert harness(_artifact(optional=True)) == 0
    assert _warning_lines(capsys) == [
        f"WARNING: optional artifact [DEB_ARM_MSAN:{_GLOB_PATH}] is missing - skipping"
    ]


def test_missing_phony_artifact_does_not_raise(harness):
    """Artifact reports are uploaded only if the provider had links.

    `_pre_run` synthesizes these with the default `optional=False`, so only the
    type filter keeps the jobs that require a JOB name (Docker server image,
    Docker keeper image, ClickHouse Server Jepsen, ClickHouse Keeper Jepsen,
    Build profile diff) working when it is absent.
    """
    assert harness(_artifact(), requires_job=True) == 0


def test_second_path_of_a_list_artifact_is_checked_on_its_own(harness):
    """Entry 1's file must not satisfy entry 2.

    ARM_FUZZERS declares 3 paths (2 globs) with `optional=False`, in PR,
    MasterCI and NightlyFuzzers.
    """
    first, second = "./ci/tmp/*.fuzz", "./ci/tmp/*.dict"
    with pytest.raises(FileNotFoundError) as ex:
        harness(
            _artifact(path=[first, second]), _FakeS3Client(names=["libfuzzer_1.fuzz"])
        )
    assert second in str(ex.value)
    assert first not in str(ex.value)
    # Entry 1 genuinely arrived, so it must still be where consumers look.
    assert (harness.dir / "libfuzzer_1.fuzz").is_file()


@pytest.mark.parametrize(
    "path, client",
    [
        (
            _EXACT_PATH,
            _FakeS3Client(
                names=["clickhouse"],
                download_error=_client_error("AccessDenied", "GetObject"),
            ),
        ),
        (
            _GLOB_PATH,
            _FakeS3Client(
                names=["pkg_1.deb"],
                list_error=_client_error("NoSuchBucket", "ListObjectsV2"),
            ),
        ),
        (
            _GLOB_PATH,
            _FakeS3Client(
                names=["pkg_1.deb"],
                download_error=_client_error("SlowDown", "GetObject"),
            ),
        ),
    ],
    ids=["access-denied", "no-such-bucket", "glob-transfer-failure"],
)
def test_operational_s3_failure_is_not_reported_as_a_missing_artifact(
    harness, path, client
):
    """Only absence becomes FileNotFoundError; the rest stay loud, because
    reporting an expired credential as "your build artifact is missing" would
    be a worse misattribution than the one this fix removes. A glob transfer
    failing part-way must not pass for a complete artifact either."""
    with pytest.raises(ClientError):
        harness(_artifact(path=path), client)


def test_lifted_traceback_reaches_the_cidb_record(tmp_path, monkeypatch):
    """The reason must reach CIDB, not just the job report page.

    `_post_run` is driven for real with CIDB replaced by a recorder: the defect
    is that the serializer reads `result.info` eagerly, so only a real call
    ordering can expose a lift that happens too late or does nothing.
    """
    monkeypatch.setattr(Settings, "TEMP_DIR", str(tmp_path), raising=False)
    monkeypatch.setattr(Settings, "OUTPUT_DIR", str(tmp_path), raising=False)
    monkeypatch.chdir(tmp_path)

    # from_dict re-reads JOB_OUTPUT_STREAM from GITHUB_OUTPUT on every load, so
    # the sink has to be set in the environment rather than only on the object.
    monkeypatch.setenv("GITHUB_OUTPUT", str(tmp_path / "gh_output"))

    env = _Environment.get()
    env.TRACEBACKS = ["Traceback: boom in _pre_run"]
    env.dump()
    assert _Environment.get().TRACEBACKS, "the traceback must survive the reload"

    recorded = {}

    class _RecordingCIDB:
        @classmethod
        def from_connection_secret(cls, connection_str):
            return cls()

        def insert(self, result, result_name_for_cidb=""):
            # Serialize exactly as the real CIDB.insert does, so what is
            # asserted is the record that would have been sent.
            recorded["rows"] = list(
                CIDB.json_data_generator(result, result_name_for_cidb)
            )
            return None

    monkeypatch.setattr("ci.praktika.runner.CIDB", _RecordingCIDB)

    result = Result(
        name="Stress test (x)",
        status=Result.Status.FAIL,
        start_time=Utils.timestamp(),
    )
    assert not result.info, "the lift must be what puts the reason on the result"

    job = Job.Config(name="Stress test (x)", runs_on=["dummy"], command="true")
    workflow = Workflow.Config(
        name="PR",
        event="pull_request",
        jobs=[job],
        enable_cache=False,
        enable_report=False,
        enable_cidb=True,
        # GH_SECRET resolves from the environment, so no live secret store.
        secrets=[
            Secret.Config(name=_CIDB_CONNECTION_SECRET, type=Secret.Type.GH_SECRET)
        ],
    )
    monkeypatch.setattr(Settings, "SECRET_CI_DB_CONNECTION", _CIDB_CONNECTION_SECRET)
    monkeypatch.setenv(_CIDB_CONNECTION_SECRET, "dummy")

    Runner()._post_run(result, workflow, job, run_exit_code=1)

    assert recorded.get("rows"), "CIDB.insert was never reached"
    record = json.loads(recorded["rows"][0])
    assert "boom in _pre_run" in record["test_context_raw"]
