"""
Tests for the core-dump collection built by `ClickHouseProc._collect_core_dumps`
and `ClickHouseService.collect_cores`.

Three properties are covered.

1. A core left by a `clickhouse-client` / `clickhouse-local` process that died on a
   fatal signal is retained. Only server cores used to be collected, because the
   collector globbed `ci/tmp/run_r*` alone, while a client spawned by a `.sh` test
   dumps into the directory `clickhouse-test` runs from, unless the test changes
   directory itself. A stateless test failing with `return code: 139` therefore
   left no core and no stack anywhere, and the crash could not be root-caused
   (`00900_long_parquet`, 2026-07-29).

2. Collected artifacts do not overwrite each other, and every core actually
   decrypts with the single key the job emits. Every artifact is uploaded under
   its basename alone (`S3.copy_file_to_s3` appends `Path(local_path).name`) and
   the per-result deduplication keys on the resolved absolute path, so two
   distinct files sharing a basename become one object and the later upload wins.
   With a key derived per directory that silently produced several different
   `aes.key.rsa` files, leaving the cores of every directory but the last one
   permanently undecryptable, and `core.<comm>.<pid>` collides across replicas
   running the same thread.

3. The collector only runs when the job actually failed, and the jobs that want
   client cores really declare their directory. Both are properties of code the
   unit tests above cannot reach without starting servers, so they are asserted
   statically from the job modules' source.
"""

import ast
import os
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.functional_tests import invert_bugfix_validation_status
from ci.jobs.scripts import clickhouse_proc as clickhouse_proc_module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.result import Result

# Read at import time so the restore of the redirected module globals can be
# asserted rather than assumed.
IMPORT_TIME_TEMP_DIR = clickhouse_proc_module.temp_dir
IMPORT_TIME_P_TEMP_DIR = clickhouse_proc_module.p_temp_dir

JOBS_DIR = Path(__file__).resolve().parent.parent / "jobs"

# The expression `functional_tests.py` must use to capture the run's verdict
# before bugfix validation rewrites it. Pinned as text against the production
# source and exercised as an expression below, so the two halves of
# `test_collect_logs_gate_sees_the_pre_inversion_verdict` cannot drift apart:
# a rewrite of production has to update this literal, which the truth table
# then re-checks. A string pin also rejects a semantically equivalent rewrite;
# that is the accepted trade and the convention this module already follows for
# `prepare_logs(all=...)` and both `client_core_path` values.
GATE_EXPR = "bool(test_result) and not test_result.is_ok()"


def _collector(monkeypatch, tmp_path, client_core_path=None):
    """A `ClickHouseProc` whose temp dir is `tmp_path`.

    `temp_dir` / `p_temp_dir` are module globals derived from the repository at
    import time; `monkeypatch` restores them after the test.
    """
    proc = ClickHouseProc.__new__(ClickHouseProc)
    proc.client_core_path = (
        str(client_core_path) if client_core_path is not None else None
    )
    monkeypatch.setattr(clickhouse_proc_module, "temp_dir", str(tmp_path))
    monkeypatch.setattr(clickhouse_proc_module, "p_temp_dir", Path(tmp_path))
    return proc


def _write_core(directory: Path, name: str, size: int = 2048) -> bytes:
    directory.mkdir(parents=True, exist_ok=True)
    payload = os.urandom(size)
    (directory / name).write_bytes(payload)
    return payload


def _upload(files):
    """Model the upload of `result.files`, in the two stages CI performs.

    `Result.upload_result_files_to_s3` deduplicates by resolved absolute path,
    then `S3.copy_file_to_s3` keys the object by basename. Returns the resulting
    objects and the basenames for which two *different* files collided, i.e. the
    ones where an upload destroys another upload's data.
    """
    deduplicated = {}
    for file in files:
        deduplicated.setdefault(str(Path(file).resolve()), file)

    objects = {}
    overwritten = []
    for file in deduplicated.values():
        name = Path(file).name
        if name in objects and Path(objects[name]).resolve() != Path(file).resolve():
            overwritten.append(name)
        objects[name] = file
    return objects, overwritten


def _job_source(name):
    """Source text and parsed tree of a job module.

    The modules are read, not imported: importing `fast_test` has import-time
    side effects, and running either `main` would start servers.
    """
    text = (JOBS_DIR / name).read_text()
    return text, ast.parse(text)


def _assignments_to(tree, attribute):
    """Every `x = ...` / `o.x = ...` statement in `tree` that assigns `attribute`."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Attribute):
                name = target.attr
            elif isinstance(target, ast.Name):
                name = target.id
            else:
                name = None
            if name == attribute:
                found.append(node)
    return found


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name} not found")


def _calls_to(tree, attribute, of=None):
    """Every call of `attribute` (optionally as `of.attribute`) inside `tree`."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == attribute:
            if of is None or (isinstance(func.value, ast.Name) and func.value.id == of):
                found.append(node)
        elif isinstance(func, ast.Name) and func.id == attribute and of is None:
            found.append(node)
    return found


def _keyword(call, name):
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword
    return None


def _gate(test_result):
    """Evaluate `GATE_EXPR`, the pinned collect-logs gate, for one result."""
    return eval(GATE_EXPR, {"test_result": test_result})  # noqa: S307


def _run(command):
    return subprocess.run(command, capture_output=True)


def test_client_core_is_collected(monkeypatch, tmp_path):
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    collected = _collector(
        monkeypatch, tmp_path, client_core_path=tmp_path
    )._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["client.core.clickhouse-clie.138694.zst.enc"], cores


def test_clickhouse_local_core_is_collected(monkeypatch, tmp_path):
    _write_core(tmp_path, "core.clickhouse-loca.99001")
    collected = _collector(
        monkeypatch, tmp_path, client_core_path=tmp_path
    )._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["client.core.clickhouse-loca.99001.zst.enc"], cores


def test_client_core_not_collected_when_directory_not_declared(monkeypatch, tmp_path):
    """A job that does not declare its client directory keeps today's behaviour."""
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    _write_core(tmp_path / "run_r0", "core.Server.1")
    collected = _collector(monkeypatch, tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["run_r0.core.Server.1.zst.enc"], cores


def test_one_aes_key_for_the_whole_job(monkeypatch, tmp_path):
    """Several directories must share one key, or all but one become unreadable.

    Asserted end to end: exactly one wrapped key is emitted, and every collected
    core decrypts with it back to the bytes that were dumped. A key count alone
    would still pass if a refactor emitted one key *path* while encrypting with
    different key *material*.
    """
    if not shutil.which("openssl") or not shutil.which("zstd"):
        pytest.skip("openssl and zstd are needed to decrypt a collected core")

    # The client directory must differ from `temp_dir`: the shared key is
    # `{temp_dir}/aes.key` and `collect_cores` defaults to
    # `{directory}/aes.key`, so if the two are the same directory the paths
    # alias and dropping the shared key from the client call is undetectable.
    # They differ in the stateless job too, where `client_core_path` is the
    # repository root and `temp_dir` is `<repo>/ci/tmp`.
    client_dir = tmp_path / "workdir"
    payloads = {
        "run_r0.core.MergeMutate.654-5182.zst.enc": _write_core(
            tmp_path / "run_r0", "core.MergeMutate.654-5182"
        ),
        "run_r1.core.MergeMutate.654-5182.zst.enc": _write_core(
            tmp_path / "run_r1", "core.MergeMutate.654-5182"
        ),
        "client.core.clickhouse-clie.138694.zst.enc": _write_core(
            client_dir, "core.clickhouse-clie.138694"
        ),
    }
    collected = _collector(
        monkeypatch, tmp_path, client_core_path=client_dir
    )._collect_core_dumps()

    keys = {str(Path(f).resolve()) for f in collected if f.endswith("aes.key.rsa")}
    assert len(keys) == 1, keys

    # Unwrapping `aes.key.rsa` needs the off-repo private key; the plaintext key
    # `Utils.encrypt` wrote beside it is the same material, so the round trip
    # runs without `private-cores.pem`.
    aes_key = Path(clickhouse_proc_module.temp_dir) / "aes.key"
    assert aes_key.exists(), aes_key

    cores = [f for f in collected if f.endswith(".zst.enc")]
    assert sorted(Path(f).name for f in cores) == sorted(payloads), cores

    decrypted_dir = tmp_path / "decrypted"
    decrypted_dir.mkdir()
    for core in cores:
        name = Path(core).name
        compressed = decrypted_dir / name[: -len(".enc")]
        plain = decrypted_dir / name[: -len(".zst.enc")]
        decrypt = _run(
            [
                "openssl", "enc", "-d", "-aes-256-cbc",
                "-in", core, "-out", str(compressed),
                "-pbkdf2", "-pass", f"file:{aes_key}",
            ]
        )
        assert decrypt.returncode == 0, f"{name}: {decrypt.stderr.decode()}"
        decompress = _run(["zstd", "-d", "-f", str(compressed), "-o", str(plain)])
        assert decompress.returncode == 0, f"{name}: {decompress.stderr.decode()}"
        assert plain.read_bytes() == payloads[name], name


def test_core_names_do_not_collide_across_directories(monkeypatch, tmp_path):
    """`core.<comm>.<pid>` repeats across replicas running the same thread."""
    _write_core(tmp_path / "run_r0", "core.MergeMutate.654-5182")
    _write_core(tmp_path / "run_r1", "core.MergeMutate.654-5182")
    collected = _collector(monkeypatch, tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    duplicated = [name for name, count in Counter(cores).items() if count > 1]
    assert not duplicated, cores


def test_no_artifact_overwrites_another(monkeypatch, tmp_path):
    """The end-to-end property: every collected file survives the upload."""
    _write_core(tmp_path / "run_r0", "core.MergeMutate.654-5182")
    _write_core(tmp_path / "run_r1", "core.MergeMutate.654-5182")
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    collected = _collector(
        monkeypatch, tmp_path, client_core_path=tmp_path
    )._collect_core_dumps()

    _, overwritten = _upload(collected)
    assert not overwritten, overwritten


def test_three_cores_kept_per_directory(monkeypatch, tmp_path):
    """The per-directory cap is unchanged: three each, so up to nine servers."""
    for replica in ("run_r0", "run_r1"):
        for i in range(5):
            _write_core(tmp_path / replica, f"core.Thread{i}.{1000 + i}", size=256)
    collected = _collector(monkeypatch, tmp_path)._collect_core_dumps()

    per_directory = Counter(
        Path(f).parent.name for f in collected if f.endswith(".zst.enc")
    )
    assert per_directory == {"run_r0": 3, "run_r1": 3}, per_directory


def test_already_compressed_cores_are_skipped(monkeypatch, tmp_path):
    run_dir = tmp_path / "run_r0"
    _write_core(run_dir, "core.Fresh.1", size=256)
    (run_dir / "core.Done.2.zst").write_bytes(b"compressed")
    (run_dir / "core.Done.3.zst.enc").write_bytes(b"encrypted")
    collected = _collector(monkeypatch, tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["run_r0.core.Fresh.1.zst.enc"], cores


def test_nothing_collected_when_no_core_present(monkeypatch, tmp_path):
    """A passing job must not gain artifacts."""
    (tmp_path / "run_r0").mkdir(parents=True)
    collected = _collector(
        monkeypatch, tmp_path, client_core_path=tmp_path
    )._collect_core_dumps()

    assert collected == [], collected
    assert not (tmp_path / "aes.key.rsa").exists()


def test_collect_cores_defaults_are_unchanged(tmp_path):
    """`ast_fuzzer_job` and `stress_job` call this with one directory and no key."""
    _write_core(tmp_path, "core.Fuzzer.7", size=256)
    collected = ClickHouseService.collect_cores(tmp_path)

    assert sorted(Path(f).name for f in collected) == [
        "aes.key.rsa",
        "core.Fuzzer.7.zst.enc",
    ], collected
    assert (tmp_path / "aes.key.rsa").exists()


def test_module_globals_are_restored():
    """The redirection above must not leak into the rest of the session.

    `ci/jobs/ci_tests_job.py` runs all of `ci/tests` in one process, so a
    permanent rebind would leave every later test with a `temp_dir` pointing at
    a deleted pytest directory.
    """
    assert clickhouse_proc_module.temp_dir == IMPORT_TIME_TEMP_DIR
    assert clickhouse_proc_module.p_temp_dir == IMPORT_TIME_P_TEMP_DIR


def test_collect_logs_gate_sees_the_pre_inversion_verdict():
    """Bugfix validation rewrites a reproduced failure to `OK` before the gate.

    `Result.is_ok` accepts `OK` and `SKIPPED`, so reading the verdict after the
    inversion makes `prepare_logs(all=...)` False on exactly the jobs where a
    crash was expected, and `_collect_core_dumps` never runs.
    """
    test_result = Result.create_from(
        name="Tests",
        results=[Result(name="00900_long_parquet", status=Result.Status.FAIL)],
    )

    assert not test_result.is_ok()
    captured = _gate(test_result)
    assert captured

    assert invert_bugfix_validation_status(test_result) is False
    # The trap: the run failed, yet the result now reports itself as ok.
    assert test_result.is_ok()
    assert captured

    # The verdict of every status the gate can see. `is_ok` accepts SKIPPED, so
    # a no-repro validation run is correctly treated as not-failed; ERROR is
    # neither FAIL nor OK, and reading it before the inversion is what makes an
    # environment-setup failure collect its logs at all.
    assert _gate(None) is False
    assert (
        _gate(
            Result.create_from(
                name="Tests", results=[Result(name="t", status=Result.Status.OK)]
            )
        )
        is False
    )
    assert _gate(Result(name="Tests", status=Result.Status.SKIPPED)) is False
    assert _gate(Result(name="Tests", status=Result.Status.ERROR)) is True

    # And the module must read the verdict before the inversion, not after, with
    # exactly the expression the truth table above exercises.
    source, tree = _job_source("functional_tests.py")
    main = _function(tree, "main")
    captures = _assignments_to(main, "test_run_failed")
    assert len(captures) == 1, ast.dump(main)[:200]
    assert (
        ast.get_source_segment(source, captures[0].value) == GATE_EXPR
    ), ast.get_source_segment(source, captures[0])
    inversions = _calls_to(main, "invert_bugfix_validation_status")
    assert len(inversions) == 1
    assert captures[0].lineno < inversions[0].lineno, (
        captures[0].lineno,
        inversions[0].lineno,
    )

    prepare_logs = _calls_to(main, "prepare_logs", of="CH")
    assert len(prepare_logs) == 1
    gate = _keyword(prepare_logs[0], "all")
    assert gate is not None and ast.get_source_segment(source, gate.value) == (
        "test_run_failed"
    ), ast.get_source_segment(source, prepare_logs[0])


def test_functional_tests_declares_its_client_core_directory():
    """`run_tests` does not change directory, so cwd is the repository root."""
    source, tree = _job_source("functional_tests.py")

    assigns = _assignments_to(tree, "client_core_path")
    assert len(assigns) == 1, [a.lineno for a in assigns]
    assert (
        ast.get_source_segment(source, assigns[0].value) == "Utils.cwd()"
    ), ast.get_source_segment(source, assigns[0])

    # The premise that makes `Utils.cwd()` the right directory.
    assert "os.chdir" not in source
    runner = _function(tree, "run_tests")
    commands = _assignments_to(runner, "command")
    assert len(commands) == 1
    command = ast.get_source_segment(source, commands[0])
    assert "clickhouse-test" in command
    assert "cd " not in command, command

    shell_runs = _calls_to(runner, "run", of="Shell")
    assert len(shell_runs) == 1
    assert _keyword(shell_runs[0], "cwd") is None, ast.get_source_segment(
        source, shell_runs[0]
    )


def test_fast_test_declares_its_client_core_directory():
    """`fast_test_command` prefixes `cd {temp_dir}`, so cwd is the temp dir."""
    source, tree = _job_source("fast_test.py")

    assigns = _assignments_to(tree, "client_core_path")
    assert len(assigns) == 1, [a.lineno for a in assigns]
    assert (
        ast.get_source_segment(source, assigns[0].value) == "str(temp_dir)"
    ), ast.get_source_segment(source, assigns[0])

    commands = _assignments_to(tree, "fast_test_command")
    assert len(commands) == 1
    command = ast.get_source_segment(source, commands[0])
    assert "clickhouse-test" in command
    assert "cd {temp_dir} &&" in command, command
