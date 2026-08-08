"""Regression tests for the download side of required S3 artifacts.

A job whose non-optional S3 artifact was never uploaded used to be dispatched
anyway, download nothing, and die 1-3 minutes later at whatever first touched
the missing file (`dpkg: error: cannot access archive 'package_folder/*.deb'`),
reported as the pull request's own workload bug. `Runner._pre_run` now verifies
presence per artifact path. A glob download is staged in an empty directory,
because a recursive `aws s3 cp` exits 0 on zero matches, so the check has to
measure that transfer rather than the shared `Settings.INPUT_DIR`.
"""

import dataclasses
import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika import Job, Workflow
from ci.praktika import s3 as s3_module
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
_ABSENT_KEY_STDERR = (
    "fatal error: An error occurred (404) when calling the HeadObject "
    'operation: Key "x" does not exist'
)
_NO_BUCKET_STDERR = (
    "fatal error: An error occurred (NoSuchBucket) when calling the "
    "ListObjectsV2 operation: The specified bucket does not exist"
)
_CIDB_SECRETS = (
    Settings.SECRET_CI_DB_URL,
    Settings.SECRET_CI_DB_USER,
    Settings.SECRET_CI_DB_PASSWORD,
)


def _environment(**overrides):
    """A minimal local-run `_Environment`; only the overridden fields matter."""
    fields = {
        f.name: ""
        for f in dataclasses.fields(_Environment)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    }
    return _Environment(**{**fields, "PR_NUMBER": 1, "LOCAL_RUN": True, **overrides})


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Drive Runner._pre_run against a stubbed S3 and a private INPUT_DIR.

    cwd is moved off the repo so `_pre_run`'s `git status --short` reports
    nothing and its `git clean -ffd` never runs.
    """
    monkeypatch.setattr(Settings, "S3_ARTIFACT_PATH", "bucket/artifacts", raising=False)
    monkeypatch.chdir(tmp_path)
    # TEMP_DIR/INPUT_DIR are user-overridable settings, so every arm runs under
    # a path containing a space: the stage is created and removed through
    # pathlib, and must not be resplit into a different `rm -rf` target.
    root = tmp_path / "ci tmp with space"
    root.mkdir()
    monkeypatch.setattr(Settings, "TEMP_DIR", str(root), raising=False)
    monkeypatch.setattr(Settings, "INPUT_DIR", str(root), raising=False)
    # `_Environment.file_name_static` reads TEMP_DIR at call time, so the dump
    # has to follow the relocation or `_pre_run` finds no environment file and
    # synthesizes a local-run dummy with PR_NUMBER=-1.
    _environment(
        WORKFLOW_NAME="PR",
        JOB_NAME="Stress test (x)",
        REPOSITORY="o/r",
        SHA="deadbeef",
        EVENT_TYPE="pull_request",
    ).dump()

    calls = []

    def run(
        artifact,
        *,
        writes=(),
        returns=True,
        requires_job=False,
        cli_stderr=None,
        cli_stages=(),
    ):
        """Run _pre_run for one artifact; the stub writes `writes` where told.

        `writes` may be a list of per-call lists, which is how a list-valued
        `path` is driven. `requires_job` makes the consumer require the
        PROVIDING JOB by name, which is how `_pre_run` is driven to synthesize a
        PHONY config. `cli_stderr` drives the REAL `copy_file_from_s3` down its
        AWS-CLI branch with the shell stubbed to that stderr, and `cli_stages`
        names files it leaves behind before failing (a partial transfer).
        """
        if cli_stderr is not None:
            monkeypatch.setattr(s3_module, "BOTO3_AVAILABLE", False, raising=False)

            def fake_shell(command, verbose=False, **kw):
                # The stage is found by prefix rather than parsed out of the
                # command, which does not quote its destination.
                for stage in Path(Settings.INPUT_DIR).glob("_artifact_download_*"):
                    for name in cli_stages:
                        (stage / name).write_text("partial")
                return (1, "", cli_stderr)

            monkeypatch.setattr(
                s3_module.Shell,
                "get_res_stdout_stderr",
                staticmethod(fake_shell),
            )
        else:

            def fake_copy(
                s3_path, local_path, recursive=False, include_pattern="", **kw
            ):
                # local_path is the stage for a glob and INPUT_DIR otherwise;
                # write where the real download would, so the caller's presence
                # check sees exactly what S3 would have delivered.
                calls.append(s3_path)
                names = writes
                if names and isinstance(names[0], list):
                    names = names[len(calls) - 1]
                for name in names:
                    (Path(local_path) / name).write_text("payload")
                return returns

            monkeypatch.setattr(S3, "copy_file_from_s3", staticmethod(fake_copy))
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
        return Runner()._pre_run(workflow, job, local_run=True)

    run.dir = root
    run.calls = calls
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


def _stages(root):
    """Leftover staging directories, which must be none after any outcome."""
    return [p.name for p in Path(root).iterdir() if p.name.startswith("_artifact_")]


def test_missing_required_glob_artifact_raises_and_a_stale_file_does_not_count(harness):
    """The production shape: the download "succeeds" but transfers nothing.

    A leftover of the same name must not pass for it - INPUT_DIR is shared by
    every artifact of the job and retained across local runs. The success
    control then proves the file still reaches where consumers look.
    """
    (harness.dir / "pkg_1.deb").write_text("stale")
    with pytest.raises(FileNotFoundError) as ex:
        harness(_artifact())
    # The message must name what is missing and who owed it, or the red check
    # stays unattributable - which is the whole point of the fix.
    assert "DEB_ARM_MSAN" in str(ex.value)
    assert "Build (arm_msan)" in str(ex.value)

    assert harness(_artifact(), writes=["pkg_1.deb"]) == 0
    assert (harness.dir / "pkg_1.deb").read_text() == "payload"
    assert _stages(harness.dir) == []


def test_a_missing_artifact_is_skipped_with_a_warning_only_when_optional(
    harness, capsys
):
    """The non-recursive 404 shape, whose False return was discarded, and the
    optional policy on both the exact-key and the glob-staged path."""
    with pytest.raises(FileNotFoundError):
        harness(_artifact(path=_EXACT_PATH), returns=False)
    assert _warning_lines(capsys) == []

    assert harness(_artifact(path=_EXACT_PATH, optional=True), returns=False) == 0
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
    type filter keeps the five jobs that require a JOB name (Docker server
    image, Docker keeper image, ClickHouse Server Jepsen, ClickHouse Keeper
    Jepsen, Build profile diff) working when it is absent. LLVM Coverage is not
    among them: it requires no job by name, and it survives a missing artifact
    through the `optional` filter instead.
    """
    assert harness(_artifact(), returns=False, requires_job=True) == 0


def test_a_leftover_at_the_stage_path_does_not_satisfy_the_check(harness, tmp_path):
    """Neither a symlink nor a crashed run's directory may pass for a download.

    A fixed stage name is not enough: `shutil.rmtree` refuses to recurse a
    symlink, so a name removed with `ignore_errors=True` and re-created with
    `exist_ok=True` survives as the link and everything visible through it
    reads as freshly downloaded.
    """
    fixed = harness.dir / "_artifact_download_stage"
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "pkg_1.deb").write_text("stale")
    fixed.symlink_to(elsewhere)

    with pytest.raises(FileNotFoundError):
        harness(_artifact())
    # The stale file must also not be promoted to where consumers look.
    assert not (harness.dir / "pkg_1.deb").exists()
    assert (elsewhere / "pkg_1.deb").is_file()

    fixed.unlink()
    fixed.mkdir()
    (fixed / "pkg_2.deb").write_text("leftover")

    with pytest.raises(FileNotFoundError):
        harness(_artifact())
    assert not (harness.dir / "pkg_2.deb").exists()


def test_second_path_of_a_list_artifact_is_checked_on_its_own(harness):
    """Per-path staging: entry 1's file must not satisfy entry 2.

    ARM_FUZZERS declares 3 paths (2 globs) with `optional=False`, in PR,
    MasterCI and NightlyFuzzers, so a stage hoisted out of the per-path loop
    would let the first entry's objects pass for a missing second entry.
    """
    first, second = "./ci/tmp/*.fuzz", "./ci/tmp/*.dict"
    with pytest.raises(FileNotFoundError) as ex:
        harness(_artifact(path=[first, second]), writes=[["libfuzzer_1.fuzz"], []])
    assert second in str(ex.value)
    assert first not in str(ex.value)
    # Entry 1 genuinely arrived, so it must still be where consumers look.
    assert (harness.dir / "libfuzzer_1.fuzz").is_file()
    assert len(harness.calls) == 2
    assert _stages(harness.dir) == []


def test_partial_transfer_is_not_reported_as_a_complete_artifact(harness):
    """A glob download that failed after staging one file still raises.

    Crediting what reached the stage would report an INCOMPLETE required
    artifact as complete and hand the job partial input - worse than the
    misattribution being fixed. This is also the glob + CLI shape:
    `copy_file_from_s3` takes boto3 only when `not recursive and not
    include_pattern`, so every glob download is a CLI download on every host.
    """
    with pytest.raises(FileNotFoundError) as ex:
        harness(
            _artifact(),
            cli_stderr=_ABSENT_KEY_STDERR,
            cli_stages=["pkg_1.deb"],
        )
    assert "DEB_ARM_MSAN" in str(ex.value)
    assert "Build (arm_msan)" in str(ex.value)
    assert not (harness.dir / "pkg_1.deb").exists()
    assert _stages(harness.dir) == []


def test_missing_required_exact_key_artifact_raises_without_boto3(harness):
    """On the AWS-CLI backend absence RAISES instead of returning False.

    An exact-key download takes that branch wherever the host python lacks
    boto3 (an optional dependency of praktika), and there a guard that only
    tested the return value never ran at all. A nonexistent BUCKET produces
    this stderr to the byte, and boto3 returns `404` for both, so that case is
    deliberately named a missing artifact; the printed `s3_path` is what keeps
    it diagnosable.
    """
    with pytest.raises(FileNotFoundError) as ex:
        harness(_artifact(path=_EXACT_PATH), cli_stderr=_ABSENT_KEY_STDERR)
    assert "DEB_ARM_MSAN" in str(ex.value)
    assert "Build (arm_msan)" in str(ex.value)
    assert "bucket/artifacts" in str(ex.value)


@pytest.mark.parametrize(
    "stderr",
    [
        "Error when retrieving credentials: aws sso login required",
        "fatal error: An error occurred (AccessDenied) when calling the "
        "ListObjectsV2 operation: Access Denied",
        'Could not connect to the endpoint URL: "https://s3.amazonaws.com/"',
        # A misconfigured S3_ARTIFACT_PATH, which shares the "does not exist"
        # marker with an absent key but is not a missing build artifact.
        _NO_BUCKET_STDERR,
    ],
    ids=["expired-login", "access-denied", "no-endpoint", "no-such-bucket"],
)
def test_operational_s3_failure_is_not_reported_as_a_missing_artifact(harness, stderr):
    """Only absence becomes FileNotFoundError; the rest stay loud, because
    reporting an expired credential as "your build artifact is missing" would
    be a worse misattribution than the one this fix removes."""
    # A bucket is named only on the recursive path, so that case runs there:
    # exact-key `HeadObject` reports one byte-identically to a missing key.
    path = _GLOB_PATH if "NoSuchBucket" in stderr else _EXACT_PATH
    with pytest.raises(RuntimeError) as ex:
        harness(_artifact(path=path), cli_stderr=stderr)
    assert not isinstance(ex.value, FileNotFoundError)
    assert "s3 command failed" in str(ex.value)


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
        def __init__(self, **kw):
            pass

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
            Secret.Config(name=name, type=Secret.Type.GH_SECRET)
            for name in _CIDB_SECRETS
        ],
    )
    for name in _CIDB_SECRETS:
        monkeypatch.setenv(name, "dummy")

    Runner()._post_run(result, workflow, job, run_exit_code=1)

    assert recorded.get("rows"), "CIDB.insert was never reached"
    record = json.loads(recorded["rows"][0])
    assert "boom in _pre_run" in record["test_context_raw"]
