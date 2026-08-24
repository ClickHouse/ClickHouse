"""Regression tests for the build profile post-hook and its producer.

See ClickHouse/ClickHouse#84159.

Two layers are covered:

* The producer (``utils/prepare-time-trace/prepare-time-trace.sh``): for a build
  with no readable objects (cross-arch/non-Linux) it must leave
  ``binary_sizes.txt`` empty, not write a junk ``0`` row that no consumer can
  parse.
* The hook artifact selection (``_has_data`` / ``_upload_profile_artifacts``):
  no-op for builds without profile data, upload for builds that have it, and
  propagate any genuine upload error so the caller fails loudly.
"""

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.job_hooks.build_profile_hook import (
    _has_data,
    _upload_profile_artifacts,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_PRODUCER = _REPO_ROOT / "utils" / "prepare-time-trace" / "prepare-time-trace.sh"


def _record_inserts(recorded):
    def insert(build_name, start_time, file):
        recorded.append(str(file))

    return insert


# --- producer: empty build dir must not emit a junk row -------------------


def test_producer_no_objects_leaves_binary_sizes_empty(tmp_path):
    """Cross-arch build: no objects -> binary_sizes.txt empty, not '0\\n'.

    Without ``xargs -r`` GNU xargs runs ``wc -c`` once with no args and writes a
    bare ``0`` row, which fails the binary_sizes FORMAT Regexp and aborts the
    upload. The producer must instead leave the file empty.
    """
    build_dir = tmp_path / "build"
    build_dir.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    subprocess.run(
        ["bash", str(_PRODUCER), str(build_dir), str(out_dir)],
        check=True,
        capture_output=True,
    )

    binary_sizes = out_dir / "binary_sizes.txt"
    assert binary_sizes.exists()
    assert binary_sizes.read_bytes() == b""


# --- hook artifact selection ----------------------------------------------


def test_has_data_false_for_missing(tmp_path):
    assert not _has_data(tmp_path / "absent.txt")


def test_has_data_false_for_empty(tmp_path):
    empty = tmp_path / "empty.txt"
    empty.write_text("")
    assert not _has_data(empty)


def test_has_data_true_for_nonempty(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("x")
    assert _has_data(f)


def test_lto_build_missing_symbols_uploads_rest(tmp_path):
    """LTO build: binary_symbols.txt absent must not abort the upload."""
    profile = tmp_path / "profile.json"
    profile.write_text("[]")
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("1 a.o")
    symbols = tmp_path / "binary_symbols.txt"  # never created for LTO builds

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "arm_release",
        "2026-06-11 00:00:00",
        [(insert, profile), (insert, sizes), (insert, symbols)],
    )

    assert recorded == [str(profile), str(sizes)]


def test_cross_arch_build_no_data_is_noop(tmp_path):
    """Cross-arch build models the real producer output: empty files, no upload.

    profile.json is absent (the hook never opens what was not produced) and
    binary_sizes.txt / binary_symbols.txt are present-but-empty, exactly what
    ``prepare-time-trace.sh`` (with ``xargs -r``) leaves for a build with no
    readable objects. The hook must no-op without failing.
    """
    sizes = tmp_path / "binary_sizes.txt"
    sizes.write_text("")  # real producer output for no objects, not absent
    symbols = tmp_path / "binary_symbols.txt"
    symbols.write_text("")

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "amd_darwin",
        "2026-06-11 00:00:00",
        [
            (insert, tmp_path / "profile.json"),
            (insert, sizes),
            (insert, symbols),
        ],
    )

    assert recorded == []


def test_native_build_uploads_all(tmp_path):
    """Non-LTO native build: all three artifacts present and uploaded."""
    files = []
    for name in ("profile.json", "binary_sizes.txt", "binary_symbols.txt"):
        f = tmp_path / name
        f.write_text("x")
        files.append(f)

    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "amd_debug",
        "2026-06-11 00:00:00",
        [(insert, f) for f in files],
    )

    assert recorded == [str(f) for f in files]


def test_upload_error_propagates(tmp_path):
    """A genuine upload failure must not be swallowed by the selector."""
    f = tmp_path / "binary_sizes.txt"
    f.write_text("1 a.o")

    def failing_insert(build_name, start_time, file):
        raise AssertionError("upload rejected")

    try:
        _upload_profile_artifacts(
            "amd_debug", "2026-06-11 00:00:00", [(failing_insert, f)]
        )
    except AssertionError:
        return
    raise AssertionError("expected the upload error to propagate")
