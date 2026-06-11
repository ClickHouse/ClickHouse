"""Regression tests for the build profile post-hook artifact selection.

See ClickHouse/ClickHouse#84159.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.job_hooks.build_profile_hook import (
    _has_data,
    _upload_profile_artifacts,
)


def _record_inserts(recorded):
    def insert(build_name, start_time, file):
        recorded.append(str(file))

    return insert


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


def test_cross_arch_build_no_artifacts_is_noop(tmp_path):
    """Cross-arch build: no readable objects, nothing produced, no failure."""
    recorded = []
    insert = _record_inserts(recorded)
    _upload_profile_artifacts(
        "amd_darwin",
        "2026-06-11 00:00:00",
        [
            (insert, tmp_path / "profile.json"),
            (insert, tmp_path / "binary_sizes.txt"),
            (insert, tmp_path / "binary_symbols.txt"),
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
