"""
Tests for the atomic Result.dump().

A `Stateless tests (amd_llvm_coverage, ...)` job whose 11882 tests all passed was
reported as a red job with a completely empty `info`. The host docker daemon died
during the post-run `llvm-profdata merge`, while `Result.dump()` was writing the
final ~1.5 MB result JSON. The inherited `Serializable.dump` opens the target with
mode "w", which truncates it to zero bytes before the first byte of the new payload
is written, so the dying process left a 0-byte file and destroyed the RUNNING result
`_pre_run` had persisted. Every subsequent read of that file raised
`JSONDecodeError: Expecting value: line 1 column 1 (char 0)`.

Writing to a sibling temp file and renaming removes that window: a reader always sees
either the previous complete content or the new complete content.

The temp file must be a *sibling* of the target. `os.replace` is atomic only within a
single filesystem, so a temp file in /tmp (or `tempfile.mkstemp()`'s default location)
turns every dump in CI into `OSError(EXDEV) Invalid cross-device link`.
"""

import errno
import json
import os
import stat
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pytest

from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import MetaClasses

JOB_NAME = "Stateless tests (amd_llvm_coverage, AsyncInsert, s3 storage, parallel)"


class _Boom(Exception):
    pass


@pytest.fixture
def in_tmp_cwd(tmp_path, monkeypatch):
    """Run with `Settings.TEMP_DIR` inside a temp dir, as praktika does from the repo root."""
    monkeypatch.chdir(tmp_path)
    os.makedirs(Settings.TEMP_DIR, exist_ok=True)
    return tmp_path


def _result(status=Result.Status.RUNNING, info=""):
    return Result(
        name=JOB_NAME, status=status, start_time=1.0, duration=None, info=info
    )


def _temp_dir_entries():
    return sorted(os.listdir(Settings.TEMP_DIR))


def test_dump_writes_the_target_file(in_tmp_cwd):
    """Baseline: dump() persists a readable result at the canonical path."""
    r = _result()
    assert r.dump() is r, "dump() must return self - callers chain .dump()"
    assert Result.from_fs(JOB_NAME).status == Result.Status.RUNNING


def test_dump_content_is_identical_to_the_base_implementation(in_tmp_cwd):
    """Only the write is staged - the file *content* and *mode* must not change.

    The mode matters because the staged write creates the file: `open(path, "w")` creates
    with 0o666 & ~umask, so the explicit `os.open` mode must be 0o666 as well. A stricter
    one (`tempfile.mkstemp()` uses 0o600) would silently narrow the result file for every
    downstream reader.
    """
    target = Result.file_name_static(JOB_NAME)
    r = _result(status=Result.Status.OK, info="all good")
    r.dump()
    atomic_bytes = open(target, "rb").read()
    atomic_mode = stat.S_IMODE(os.stat(target).st_mode)

    os.remove(target)
    MetaClasses.Serializable.dump(r)
    base_bytes = open(target, "rb").read()
    base_mode = stat.S_IMODE(os.stat(target).st_mode)

    assert atomic_bytes == base_bytes
    assert oct(atomic_mode) == oct(base_mode)


def test_failed_dump_keeps_the_previous_content(in_tmp_cwd):
    """The load-bearing assertion: a write that dies partway must not destroy the
    result already on disk. Fails if dump() writes the target in place."""
    _result(status=Result.Status.RUNNING, info="pre-run").dump()

    def _partial_then_raise(obj, f, **kwargs):
        f.write('{"name": "partial"')
        raise _Boom("killed mid-dump")

    # A nested context (not monkeypatch.undo()) so the fixture's chdir stays in effect.
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(json, "dump", _partial_then_raise)
        with pytest.raises(_Boom):
            _result(status=Result.Status.OK, info="never landed").dump()

    recovered = Result.from_fs(JOB_NAME)
    assert recovered.status == Result.Status.RUNNING
    assert recovered.info == "pre-run"
    assert recovered.is_completed() is False
    assert recovered.is_running() is True


def test_failed_dump_leaves_no_temp_file(in_tmp_cwd):
    """A raising json.dump must not leave litter beside the target."""
    _result().dump()
    before = _temp_dir_entries()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            json, "dump", lambda obj, f, **kwargs: (_ for _ in ()).throw(_Boom("boom"))
        )
        with pytest.raises(_Boom):
            _result(status=Result.Status.OK).dump()

    assert _temp_dir_entries() == before
    assert not [e for e in _temp_dir_entries() if ".tmp" in e]


def _spy_replace(mp):
    """Record the (src, dst) of every os.replace performed while the patch is active."""
    seen = []
    real_replace = os.replace

    def _spy(src, dst, **kwargs):
        seen.append((str(src), str(dst)))
        return real_replace(src, dst, **kwargs)

    mp.setattr(os, "replace", _spy)
    return seen


def test_temp_file_is_a_sibling_of_the_target(in_tmp_cwd):
    """`os.replace` is atomic only within one filesystem, so the temp file must live in
    the target's own directory. A temp in /tmp raises OSError(EXDEV) in CI."""
    with pytest.MonkeyPatch.context() as mp:
        seen = _spy_replace(mp)
        _result().dump()

    target = Result.file_name_static(JOB_NAME)
    assert seen, "dump() must rename a temp file onto the target"
    src, dst = seen[0]
    assert os.path.abspath(dst) == os.path.abspath(target)
    assert os.path.dirname(os.path.abspath(src)) == os.path.dirname(
        os.path.abspath(target)
    )
    assert os.path.exists(target)


def test_temp_name_is_unique_per_process(in_tmp_cwd):
    """Concurrent dumps of the same result from different processes must not collide."""
    with pytest.MonkeyPatch.context() as mp:
        seen = _spy_replace(mp)
        _result().dump()
    assert str(os.getpid()) in os.path.basename(seen[0][0])


def test_dump_refuses_a_symlinked_temp_path(in_tmp_cwd, tmp_path):
    """The temp name is derived from the result path and this pid, so it is guessable. A
    pre-existing symlink there must be refused, not written through."""
    victim = tmp_path / "victim_outside_workspace"
    victim.write_text("do-not-clobber")
    _result(status=Result.Status.RUNNING, info="pre-run").dump()

    target = Result.file_name_static(JOB_NAME)
    os.symlink(victim, f"{target}.tmp.{os.getpid()}")

    with pytest.raises(OSError) as ex:
        _result(status=Result.Status.OK, info="never landed").dump()
    assert ex.value.errno == errno.ELOOP

    assert victim.read_text() == "do-not-clobber"
    # and the previously persisted result is still readable
    assert Result.from_fs(JOB_NAME).info == "pre-run"


def test_dump_reuses_a_stale_regular_temp_file(in_tmp_cwd):
    """`O_EXCL` is deliberately absent: a leftover temp from a previously killed dump
    must stay reusable, or crash recovery would itself crash.

    Kills the mutant that adds `| os.O_EXCL` to the `os.open` flags - it makes this
    dump() raise FileExistsError (errno 17). `test_dump_refuses_a_symlinked_temp_path`
    cannot substitute: O_EXCL rejects a pre-existing symlink too (with EEXIST rather
    than ELOOP), so it fails under that mutant for the wrong reason and never observes
    a *regular* leftover.
    """
    _result(status=Result.Status.RUNNING, info="pre-run").dump()

    target = Result.file_name_static(JOB_NAME)
    leftover = f"{target}.tmp.{os.getpid()}"
    with open(leftover, "w", encoding="utf8") as f:
        f.write('{"name": "half-written"')

    _result(status=Result.Status.OK, info="landed").dump()

    landed = Result.from_fs(JOB_NAME)
    assert landed.status == Result.Status.OK
    assert landed.info == "landed"
    # os.replace consumed the reused temp, so no litter is left behind.
    assert not [e for e in _temp_dir_entries() if ".tmp" in e]


def test_dump_of_a_result_with_subresults_round_trips(in_tmp_cwd):
    """The observed failure carried 11882 sub-results; nesting must survive the rename."""
    r = _result(status=Result.Status.OK)
    r.results = [
        Result(name=f"test_{i}", status=Result.Status.OK, start_time=1.0, duration=0.1)
        for i in range(50)
    ]
    r.dump()
    got = Result.from_fs(JOB_NAME)
    assert len(got.results) == 50
    assert [x.name for x in got.results] == [x.name for x in r.results]
