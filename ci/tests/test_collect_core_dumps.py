"""
Tests for the core-dump collection built by `ClickHouseProc._collect_core_dumps`
and `ClickHouseService.collect_cores`.

Two properties are covered.

1. A core left by a `clickhouse-client` / `clickhouse-local` process that died on a
   fatal signal is retained. Only server cores used to be collected, because the
   collector globbed `ci/tmp/run_r*` alone, while a client spawned by a `.sh` test
   dumps into the directory `clickhouse-test` runs from. A stateless test failing
   with `return code: 139` therefore left no core and no stack anywhere, and the
   crash could not be root-caused (`00900_long_parquet`, 2026-07-29).

2. Collected artifacts do not overwrite each other. Every artifact is uploaded
   under its basename alone (`S3.copy_file_to_s3` appends `Path(local_path).name`)
   and the per-result deduplication keys on the resolved absolute path, so two
   distinct files sharing a basename become one object and the later upload wins.
   With a key derived per directory that silently produced several different
   `aes.key.rsa` files, leaving the cores of every directory but the last one
   permanently undecryptable, and `core.<comm>.<pid>` collides across replicas
   running the same thread.
"""

import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts import clickhouse_proc as clickhouse_proc_module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.clickhouse_service import ClickHouseService


def _collector(tmp_path, client_core_path=None):
    """A `ClickHouseProc` whose temp dir is `tmp_path`.

    The module-level `temp_dir` / `p_temp_dir` are derived from the repository at
    import time, so they are redirected for the duration of the test.
    """
    proc = ClickHouseProc.__new__(ClickHouseProc)
    proc.client_core_path = (
        str(client_core_path) if client_core_path is not None else None
    )
    clickhouse_proc_module.temp_dir = str(tmp_path)
    clickhouse_proc_module.p_temp_dir = Path(tmp_path)
    return proc


def _write_core(directory: Path, name: str, size: int = 2048) -> bytes:
    directory.mkdir(parents=True, exist_ok=True)
    payload = os.urandom(size)
    (directory / name).write_bytes(payload)
    return payload


def _upload(files):
    """Model the upload of `result.files`, in the two stages CI performs.

    `Result.upload_files` deduplicates by resolved absolute path, then
    `S3.copy_file_to_s3` keys the object by basename. Returns the resulting
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


def test_client_core_is_collected(tmp_path):
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    collected = _collector(tmp_path, client_core_path=tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["client.core.clickhouse-clie.138694.zst.enc"], cores


def test_clickhouse_local_core_is_collected(tmp_path):
    _write_core(tmp_path, "core.clickhouse-loca.99001")
    collected = _collector(tmp_path, client_core_path=tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["client.core.clickhouse-loca.99001.zst.enc"], cores


def test_client_core_not_collected_when_directory_not_declared(tmp_path):
    """A job that does not declare its client directory keeps today's behaviour."""
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    _write_core(tmp_path / "run_r0", "core.Server.1")
    collected = _collector(tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["run_r0.core.Server.1.zst.enc"], cores


def test_one_aes_key_for_the_whole_job(tmp_path):
    """Several directories must share one key, or all but one become unreadable."""
    _write_core(tmp_path / "run_r0", "core.MergeMutate.654-5182")
    _write_core(tmp_path / "run_r1", "core.MergeMutate.654-5182")
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    collected = _collector(tmp_path, client_core_path=tmp_path)._collect_core_dumps()

    keys = {str(Path(f).resolve()) for f in collected if f.endswith("aes.key.rsa")}
    assert len(keys) == 1, keys


def test_core_names_do_not_collide_across_directories(tmp_path):
    """`core.<comm>.<pid>` repeats across replicas running the same thread."""
    _write_core(tmp_path / "run_r0", "core.MergeMutate.654-5182")
    _write_core(tmp_path / "run_r1", "core.MergeMutate.654-5182")
    collected = _collector(tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    duplicated = [name for name, count in Counter(cores).items() if count > 1]
    assert not duplicated, cores


def test_no_artifact_overwrites_another(tmp_path):
    """The end-to-end property: every collected file survives the upload."""
    _write_core(tmp_path / "run_r0", "core.MergeMutate.654-5182")
    _write_core(tmp_path / "run_r1", "core.MergeMutate.654-5182")
    _write_core(tmp_path, "core.clickhouse-clie.138694")
    collected = _collector(tmp_path, client_core_path=tmp_path)._collect_core_dumps()

    _, overwritten = _upload(collected)
    assert not overwritten, overwritten


def test_three_cores_kept_per_directory(tmp_path):
    """The per-directory cap is unchanged: three each, so up to nine servers."""
    for replica in ("run_r0", "run_r1"):
        for i in range(5):
            _write_core(tmp_path / replica, f"core.Thread{i}.{1000 + i}", size=256)
    collected = _collector(tmp_path)._collect_core_dumps()

    per_directory = Counter(
        Path(f).parent.name for f in collected if f.endswith(".zst.enc")
    )
    assert per_directory == {"run_r0": 3, "run_r1": 3}, per_directory


def test_already_compressed_cores_are_skipped(tmp_path):
    run_dir = tmp_path / "run_r0"
    _write_core(run_dir, "core.Fresh.1", size=256)
    (run_dir / "core.Done.2.zst").write_bytes(b"compressed")
    (run_dir / "core.Done.3.zst.enc").write_bytes(b"encrypted")
    collected = _collector(tmp_path)._collect_core_dumps()

    cores = [Path(f).name for f in collected if f.endswith(".zst.enc")]
    assert cores == ["run_r0.core.Fresh.1.zst.enc"], cores


def test_nothing_collected_when_no_core_present(tmp_path):
    """A passing job must not gain artifacts."""
    (tmp_path / "run_r0").mkdir(parents=True)
    collected = _collector(tmp_path, client_core_path=tmp_path)._collect_core_dumps()

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
