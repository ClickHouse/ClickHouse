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

The two production seams that reach those arguments are pinned as well, because
each can drop the fix on its own: `main` forwarding the parsed arguments into
`run_func_test`, and the `stress.py` invocation in `stress_runner.sh`. The
pre-existing `--encrypted-storage` rides both seams and the same skip-tag
mechanism, and `no-encrypted-storage` only becomes live again once a backend
flag survives them too, so it is pinned at both seams rather than only at the
command line.
"""

import inspect
import os
import re
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


_FORWARDED = ("s3_storage", "azure_blob_storage", "encrypted_storage")


@pytest.mark.parametrize(
    "argv_flags,expected",
    [
        pytest.param(
            [
                "--s3-storage",
                "1",
                "--azure-blob-storage",
                "0",
                "--encrypted-storage",
                "1",
            ],
            (True, False, True),
            id="s3-encrypted",
        ),
        pytest.param(
            [
                "--s3-storage",
                "0",
                "--azure-blob-storage",
                "1",
                "--encrypted-storage",
                "0",
            ],
            (False, True, False),
            id="azure",
        ),
        pytest.param([], (False, False, False), id="absent"),
    ],
)
def test_cli_arguments_reach_run_func_test(monkeypatch, tmp_path, argv_flags, expected):
    """First production seam: `main` forwards the parsed arguments positionally.
    Dropping either one from that call leaves every command-construction test
    above green, because they call `run_func_test` directly. `encrypted_storage`
    rides the same seam and the same skip-tag mechanism, so it is pinned here
    too."""
    captured = []

    monkeypatch.setattr(
        stress, "run_func_test", lambda *args: captured.append(args) or []
    )
    monkeypatch.setattr(stress, "call_with_retry", lambda *a, **k: None)
    monkeypatch.setattr(stress, "compress_stress_logs", lambda *a, **k: None)

    class _Killer:
        def __init__(self, *a, **k):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(stress, "RandomQueryKiller", _Killer)
    monkeypatch.setattr(
        sys, "argv", ["stress.py", "--output-folder", str(tmp_path)] + argv_flags
    )

    stress.main()

    assert len(captured) == 1, f"expected one run_func_test call, got {captured}"
    # Resolve by parameter name: hardcoded offsets would silently pass if a
    # parameter were inserted ahead of them.
    parameters = list(inspect.signature(run_func_test).parameters)
    args = captured[0]
    indexes = [parameters.index(name) for name in _FORWARDED]
    assert len(args) > max(indexes), (
        f"main passed only {len(args)} positional arguments, so it never reaches "
        f"{parameters[max(indexes)]}: {args}"
    )
    actual = tuple(args[index] for index in indexes)
    assert actual == expected, (
        f"main forwarded {dict(zip(_FORWARDED, actual))}, "
        f"expected {dict(zip(_FORWARDED, expected))}"
    )


def test_stress_runner_forwards_every_backend():
    """Second production seam: the shell invocation. `stress.py` defaults the
    backend arguments to false, so a flag missing here disables the fix
    silently. `--encrypted-storage` is asserted alongside them because the same
    edit drops it just as quietly."""
    runner = os.path.join(
        os.path.dirname(__file__), "../..", "tests/docker_scripts/stress_runner.sh"
    )
    with open(runner, "r", encoding="utf-8") as file:
        text = file.read()

    match = re.search(r"^python3 [^\n]*stress\.py[^\n]*", text, re.M)
    assert match, f"no stress.py invocation found in {runner}"
    line = match.group(0)
    # Whole tokens, never substrings: "--s3-storage" is also a substring of
    # "--no-s3-storage".
    tokens = line.split()

    # Values are pinned exactly: an unset variable reaches argparse as "", whose
    # `type=lambda x: bool(int(x))` exits rc=2 before the first test runs. Hence
    # `:-0` on the two unexported backend variables, and the export check below.
    for flag, value_token in (
        ("--s3-storage", "${USE_S3_STORAGE_FOR_MERGE_TREE:-0}"),
        ("--azure-blob-storage", "${USE_AZURE_STORAGE_FOR_MERGE_TREE:-0}"),
        ("--encrypted-storage", "$USE_ENCRYPTED_STORAGE"),
    ):
        assert flag in tokens, f"stress_runner.sh does not pass {flag}: {line}"
        value = tokens[tokens.index(flag) + 1].strip('"')
        assert value == value_token, f"{flag} takes {value}, not {value_token}"

    assert re.search(
        r"^export USE_ENCRYPTED_STORAGE=", text, re.M
    ), "USE_ENCRYPTED_STORAGE is no longer exported, so its unguarded use exits rc=2"
