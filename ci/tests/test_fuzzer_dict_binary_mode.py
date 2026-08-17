"""
Regression test for the release binary's mode in generate_dictionary
(ci/jobs/libfuzzer_test_check.py).

The libFuzzer job stages the release binary next to the fuzzer targets and makes
it executable so it can generate all.dict. tests/fuzz/runner.py then enumerates
fuzzers as every executable *file* in that same directory, so a binary left
executable is picked up as a fuzzer target and invoked as
`./clickhouse -artifact_prefix=... corpus/clickhouse`, which the server rejects
with UNRECOGNIZED_ARGUMENTS. That produced a `clickhouse` FAIL leaf beside 20
passing *_fuzzer leaves.

The binary arrives non-executable: artifacts are downloaded with `aws s3 cp`, a
content copy that does not carry POSIX mode, which is also why the *_fuzzer
files have to be chmod-ed explicitly. So the execute bit exists only for the
duration of dictionary generation, and restoring the original mode afterwards
returns the directory to a state where only fuzzer targets are executable.

These tests pin the whole 0644 -> 0755 -> 0644 cycle rather than just the end
state: the binary must be executable *while* the generator runs, and must not be
afterwards, including when generation fails.
"""

import os
import stat
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.libfuzzer_test_check as libfuzzer_test_check  # noqa: E402

# What `aws s3 cp` actually delivers under the job's umask 0022.
_DOWNLOADED_MODE = 0o644


def _mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


@pytest.fixture
def fuzzers_path(tmp_path):
    """A staged fuzzers directory: a non-executable binary and a fuzzer target."""
    path = tmp_path / "fuzzers"
    path.mkdir()
    (path / "clickhouse").write_bytes(b"binary")
    os.chmod(path / "clickhouse", _DOWNLOADED_MODE)
    (path / "lexer_fuzzer").write_bytes(b"target")
    os.chmod(path / "lexer_fuzzer", 0o777)
    return path


def _run(monkeypatch, fuzzers_path, tmp_path, *, fail=False, restore=True):
    """Run generate_dictionary with docker stubbed out.

    The stub records the binary's mode while the generator is running, which is
    the half that must not regress. `fail` makes the stub raise, to exercise the
    failure path. `restore` False strips the restore, so the arms can be
    compared: if they agree, this test measures nothing.
    """
    observed = {}

    def fake_check_call(cmd, shell=False):  # noqa: ARG001
        observed["mode_during"] = _mode(fuzzers_path / "clickhouse")
        if fail:
            raise subprocess.CalledProcessError(1, cmd)
        return 0

    monkeypatch.setattr(libfuzzer_test_check.subprocess, "check_call", fake_check_call)

    if restore:
        libfuzzer_test_check.generate_dictionary(fuzzers_path, tmp_path, "image")
    else:
        # The pre-change code: chmod +x, run, never restore.
        binary = fuzzers_path / "clickhouse"
        binary.chmod(binary.stat().st_mode | 0o111)
        fake_check_call("docker run ...", shell=True)

    return observed


def test_binary_is_executable_during_generation(monkeypatch, fuzzers_path, tmp_path):
    observed = _run(monkeypatch, fuzzers_path, tmp_path)
    assert (
        observed["mode_during"] & 0o111
    ), "the binary must be executable while the dictionary is generated"


def test_binary_mode_is_restored(monkeypatch, fuzzers_path, tmp_path):
    _run(monkeypatch, fuzzers_path, tmp_path)
    assert _mode(fuzzers_path / "clickhouse") == _DOWNLOADED_MODE


def test_binary_mode_is_restored_when_generation_fails(
    monkeypatch, fuzzers_path, tmp_path
):
    with pytest.raises(subprocess.CalledProcessError):
        _run(monkeypatch, fuzzers_path, tmp_path, fail=True)
    assert _mode(fuzzers_path / "clickhouse") == _DOWNLOADED_MODE


def test_fuzzer_targets_stay_executable(monkeypatch, fuzzers_path, tmp_path):
    # Only the release binary's mode is touched.
    _run(monkeypatch, fuzzers_path, tmp_path)
    assert _mode(fuzzers_path / "lexer_fuzzer") & 0o111


def test_without_the_restore_the_binary_stays_executable(
    monkeypatch, fuzzers_path, tmp_path
):
    # The mutation arm: the same assertion as test_binary_mode_is_restored must
    # fail against the pre-change code, or that test proves nothing.
    _run(monkeypatch, fuzzers_path, tmp_path, restore=False)
    assert _mode(fuzzers_path / "clickhouse") != _DOWNLOADED_MODE
    assert _mode(fuzzers_path / "clickhouse") & 0o111
