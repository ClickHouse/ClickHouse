"""
Tests for the storage-backend flags `ci.jobs.scripts.stress.stress.run_func_test`
puts on the `clickhouse-test` command line.

`Stress test (*)` installs an object-storage default MergeTree policy, but for a
long time it invoked `clickhouse-test` without saying which backend it had
configured. `clickhouse-test` decides `no-object-storage` / `no-s3-storage` /
`no-azure-blob-storage` / `no-encrypted-storage` purely from those flags, so the
gates never fired and tests that declare they cannot run on object storage ran on
it anyway. Issue #111053 is the visible damage: a test writing a raw plaintext
file into a part directory poisons the table, because on object storage that file
is not in `DiskObjectStorageMetadata` format, and every later server start fails.

Command construction is this fix's entire payload, so it is what these tests pin:
`run_func_test` must put `--s3-storage` / `--azure-blob-storage` on both the smoke
check and the stress command exactly when the corresponding argument is true, and
on neither otherwise. Nothing here starts a server or runs a test: the three
side-effecting callables are patched out, and the command list is fully built
before the first of them is reached.
"""

import inspect
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.stress.stress import run_func_test
from ci.jobs.scripts.stress import stress


class _FinishedProcess:
    """Stub for `Popen`: already exited, so the wait loop ends on its first pass."""

    returncode = 0

    def poll(self):
        return self.returncode


def _capture(monkeypatch):
    """Patch out everything that touches a shell or a server.

    Returns (smoke_commands, stress_commands), both filled in by `run_func_test`.
    """
    smoke = []
    stress_cmds = []

    def fake_execute_bash(full_command, timeout=120):  # pylint:disable=unused-argument
        smoke.append(full_command)
        return ""

    def fake_popen(command, **_kwargs):
        stress_cmds.append(command)
        return _FinishedProcess()

    monkeypatch.setattr(stress, "execute_bash", fake_execute_bash)
    monkeypatch.setattr(stress, "install_thread_pool_fault_injection", lambda: None)
    monkeypatch.setattr(stress, "Popen", fake_popen)
    return smoke, stress_cmds


def _run(monkeypatch, tmp_path, **kwargs):
    smoke, stress_cmds = _capture(monkeypatch)
    run_func_test(
        "clickhouse-test",
        tmp_path,
        1,
        "",
        1200,
        False,
        kwargs.pop("encrypted_storage", False),
        **kwargs,
    )
    assert len(smoke) == 1, f"expected one smoke command, got {smoke}"
    assert len(stress_cmds) == 1, f"expected one stress command, got {stress_cmds}"
    return smoke[0], stress_cmds[0]


# Match whitespace-delimited tokens, never `flag in command`: the substring form
# is also true for `--no-s3-storage`, which would make every absence assertion
# below pass vacuously.
def _tokens(command):
    return command.split()


_ALL_FLAGS = ("--s3-storage", "--azure-blob-storage", "--encrypted-storage")


@pytest.mark.parametrize(
    "s3_storage,azure_blob_storage,encrypted_storage,expected",
    [
        pytest.param(False, False, False, (), id="local"),
        pytest.param(True, False, False, ("--s3-storage",), id="s3"),
        pytest.param(False, True, False, ("--azure-blob-storage",), id="azure"),
        pytest.param(
            True,
            False,
            True,
            ("--s3-storage", "--encrypted-storage"),
            id="s3-encrypted",
        ),
    ],
)
def test_backend_flags_on_both_commands(
    monkeypatch, tmp_path, s3_storage, azure_blob_storage, encrypted_storage, expected
):
    """Both commands share `base_command`, and an edit could easily reach only one
    of them, so presence and absence are asserted on each separately."""
    smoke, stress_cmd = _run(
        monkeypatch,
        tmp_path,
        s3_storage=s3_storage,
        azure_blob_storage=azure_blob_storage,
        encrypted_storage=encrypted_storage,
    )
    for command, label in ((smoke, "smoke check"), (stress_cmd, "stress")):
        tokens = _tokens(command)
        for flag in expected:
            assert flag in tokens, f"{label} command lacks {flag}: {command}"
        for flag in _ALL_FLAGS:
            if flag not in expected:
                assert (
                    flag not in tokens
                ), f"{label} command carries {flag} it was not asked for: {command}"


def test_both_backends_forwarded_together(monkeypatch, tmp_path):
    """`stress_runner.sh` passes both arguments on every run, so the two flags
    must be independent rather than mutually exclusive."""
    smoke, stress_cmd = _run(
        monkeypatch, tmp_path, s3_storage=True, azure_blob_storage=True
    )
    for command in (smoke, stress_cmd):
        tokens = _tokens(command)
        assert "--s3-storage" in tokens
        assert "--azure-blob-storage" in tokens


def test_flags_default_off_for_positional_callers(monkeypatch, tmp_path):
    """`upgrade_runner.sh` reaches `stress.py` without the new arguments; its
    command line has to stay byte-identical to what it was before."""
    smoke, stress_cmd = _run(monkeypatch, tmp_path)
    for command in (smoke, stress_cmd):
        tokens = _tokens(command)
        assert "--s3-storage" not in tokens
        assert "--azure-blob-storage" not in tokens


def test_query_killer_stays_last_parameter():
    """`main` passes `query_killer` positionally, so a parameter inserted after it
    would silently bind the killer to a storage flag: no exception, no lint
    warning, and the flags would follow the killer's truthiness."""
    parameters = list(inspect.signature(run_func_test).parameters)
    assert parameters[-3:] == ["s3_storage", "azure_blob_storage", "query_killer"]
