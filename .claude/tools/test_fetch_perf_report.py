#!/usr/bin/env python3
"""Tests for the query classification in fetch_perf_report.py.

These prove that the helper classifies queries with the same effective
per-query thresholds as ci/jobs/scripts/perf/compare.sh, including the cases
where the historical / per-test thresholds raise changed_threshold or
unstable_threshold above the 0.15 / 0.25 floors. In those cases classifying
with the floor constants alone would produce false "changed" / "unstable"
findings that CI treats as noise.

The test runs clickhouse-local through the same SQL builders the tool uses, so
it exercises the real classification logic. It is skipped when the clickhouse
binary is not available.

Run directly:  python3 .claude/tools/test_fetch_perf_report.py
"""

import importlib.util
import os
import shutil
import tempfile
import types

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "fetch_perf_report", os.path.join(_HERE, "fetch_perf_report.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fpr = _load_module()


def _args(**overrides):
    defaults = dict(
        metric="client_time",
        arch="all",
        shard=None,
        test=None,
        query=None,
        sort="diff",
        show_all=True,
    )
    defaults.update(overrides)
    return types.SimpleNamespace(**defaults)


# Each row mirrors one all-query-metrics.tsv record (after the arch/shard_num
# columns are prepended by download_shard):
#   arch shard metric left right diff times stat test qidx qname c_thr u_thr
# The interesting rows are "noise_below_raised_changed" and
# "stable_below_raised_unstable": with floor-only logic they would be flagged,
# but their per-query thresholds are above the floor, so CI ignores them.
ROWS = [
    # name, diff, stat_threshold, changed_threshold, unstable_threshold
    ("changed_slower", 0.30, 0.05, 0.20, 0.25),
    ("noise_below_raised_changed", 0.18, 0.05, 0.20, 0.25),
    ("unstable", 0.05, 0.30, 0.20, 0.25),
    ("stable_below_raised_unstable", 0.05, 0.28, 0.20, 0.30),
    ("changed_faster", -0.30, 0.05, 0.20, 0.25),
]


def _expected(diff, stat, changed_thr, unstable_thr):
    """Replicate the compare.sh changed_fail / unstable_fail classification."""
    is_changed = abs(diff) > changed_thr and abs(diff) >= stat
    is_unstable = (not is_changed) and stat > unstable_thr
    direction = "slower" if diff > 0 else ("faster" if diff < 0 else "same")
    return is_changed, is_unstable, direction


def _write_fixture(path):
    with open(path, "w") as f:
        for i, (name, diff, stat, c_thr, u_thr) in enumerate(ROWS):
            left, right = 1.0, 1.0 + diff
            times = abs(diff) + 1.0
            fields = [
                "amd", "1", "client_time",
                f"{left}", f"{right}", f"{diff}", f"{times}", f"{stat}",
                "test_a", str(i), name, f"{c_thr}", f"{u_thr}",
            ]
            f.write("\t".join(fields) + "\n")


def test_classification_matches_compare_sh():
    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    tmpdir = tempfile.mkdtemp(prefix="test_perf_report_")
    try:
        data_path = os.path.join(tmpdir, "all.tsv")
        _write_fixture(data_path)

        assert fpr.count_tsv_columns(data_path) == fpr.COLUMNS_WITH_THRESHOLDS

        args = _args()
        sql = fpr.build_detail_sql(args, data_path, has_thresholds=True)
        rows = {r["query"]: r for r in fpr.parse_jsonl(fpr.run_ch(sql))}

        assert len(rows) == len(ROWS), rows

        for name, diff, stat, c_thr, u_thr in ROWS:
            exp_changed, exp_unstable, exp_dir = _expected(diff, stat, c_thr, u_thr)
            row = rows[name]
            assert bool(row["is_changed"]) == exp_changed, (name, row)
            assert bool(row["is_unstable"]) == exp_unstable, (name, row)
            assert row["direction"] == exp_dir, (name, row)

        # The two rows whose thresholds were raised above the floors must NOT be
        # flagged - this is exactly what the old floor-only logic got wrong.
        assert not rows["noise_below_raised_changed"]["is_changed"]
        assert not rows["noise_below_raised_changed"]["is_unstable"]
        assert not rows["stable_below_raised_unstable"]["is_unstable"]
        assert not rows["stable_below_raised_unstable"]["is_changed"]

        # Sanity: floor-only classification (has_thresholds=False) WOULD flag
        # them, which is the regression this change fixes.
        floor_sql = fpr.build_detail_sql(args, data_path, has_thresholds=False)
        floor_rows = {r["query"]: r for r in fpr.parse_jsonl(fpr.run_ch(floor_sql))}
        assert floor_rows["noise_below_raised_changed"]["is_changed"]
        assert floor_rows["stable_below_raised_unstable"]["is_unstable"]
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_summary_counts():
    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    tmpdir = tempfile.mkdtemp(prefix="test_perf_report_")
    try:
        data_path = os.path.join(tmpdir, "all.tsv")
        _write_fixture(data_path)
        shard_meta = [{"name": "amd 1/1", "arch": "amd", "shard_num": 1}]
        sql = fpr.build_summary_sql(_args(), shard_meta, data_path, has_thresholds=True)
        summary = fpr.parse_jsonl(fpr.run_ch(sql))
        assert len(summary) == 1, summary
        s = summary[0]
        # changed_slower + changed_faster = 1 slower + 1 faster; 1 unstable.
        assert s["faster"] == 1, s
        assert s["slower"] == 1, s
        assert s["unstable"] == 1, s
        assert s["total"] == len(ROWS), s
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_maybe_decompress_handles_plain_gzip_zstd():
    import gzip as _gzip
    import subprocess as _sp

    plain = b"metric\tleft\tright\nmemory_usage\t100\t90\n"
    assert fpr.maybe_decompress(plain) == plain
    assert fpr.maybe_decompress(_gzip.compress(plain)) == plain
    zst = _sp.run(["zstd", "-cq"], input=plain, capture_output=True).stdout
    assert zst[:4] == b"\x28\xb5\x2f\xfd"  # sanity: really zstd-framed
    assert fpr.maybe_decompress(zst) == plain


def test_stream_to_file_handles_plain_gzip_zstd():
    import gzip as _gzip
    import io as _io
    import subprocess as _sp

    plain = b"metric\tleft\tright\n" + b"memory_usage\t100\t90\n" * 10000  # >4 bytes, multi-chunk
    zst = _sp.run(["zstd", "-cq"], input=plain, capture_output=True).stdout
    variants = {"plain": plain, "gzip": _gzip.compress(plain), "zstd": zst}

    with tempfile.TemporaryDirectory() as tmp:
        for name, body in variants.items():
            dest = os.path.join(tmp, name)
            # A BytesIO stands in for the urlopen response: same .read(size) contract.
            fpr._stream_to_file(_io.BytesIO(body), dest)
            with open(dest, "rb") as f:
                assert f.read() == plain, name


def test_stream_to_file_zstd_cli_fallback():
    # Force the `zstd` CLI path even where `zstandard` is installed, so the fallback the PR
    # advertises is exercised regardless of the host's optional dependencies.
    import io as _io
    import subprocess as _sp
    import sys as _sys

    plain = b"metric\tleft\tright\n" + b"memory_usage\t100\t90\n" * 10000
    zst = _sp.run(["zstd", "-cq"], input=plain, capture_output=True).stdout

    saved = _sys.modules.get("zstandard", "MISSING")
    _sys.modules["zstandard"] = None  # makes `import zstandard` raise ImportError
    try:
        with tempfile.TemporaryDirectory() as tmp:
            dest = os.path.join(tmp, "out")
            fpr._stream_to_file(_io.BytesIO(zst), dest)
            with open(dest, "rb") as f:
                assert f.read() == plain
    finally:
        if saved == "MISSING":
            _sys.modules.pop("zstandard", None)
        else:
            _sys.modules["zstandard"] = saved


def test_stream_to_file_zstd_cli_times_out():
    # A wedged zstd must not hang the report forever: the watchdog kills it and the failure
    # becomes a normal per-shard error. Uses a fake `zstd` on PATH that ignores stdin and never
    # exits, plus a shrunk timeout so the test stays fast.
    import io as _io
    import stat as _stat
    import sys as _sys
    import time as _time

    saved_mod = _sys.modules.get("zstandard", "MISSING")
    saved_path = os.environ["PATH"]
    saved_timeout = fpr._ZSTD_CLI_TIMEOUT_SEC
    _sys.modules["zstandard"] = None  # force the CLI fallback
    with tempfile.TemporaryDirectory() as tmp:
        fake = os.path.join(tmp, "zstd")
        with open(fake, "w") as f:
            f.write("#!/bin/sh\nexec sleep 300\n")
        os.chmod(fake, os.stat(fake).st_mode | _stat.S_IEXEC)
        os.environ["PATH"] = tmp + os.pathsep + saved_path
        fpr._ZSTD_CLI_TIMEOUT_SEC = 2
        try:
            zst_magic_input = b"\x28\xb5\x2f\xfd" + b"\x00" * 4096
            dest = os.path.join(tmp, "out")
            t0 = _time.time()
            try:
                fpr._stream_to_file(_io.BytesIO(zst_magic_input), dest)
                assert False, "expected the wedged zstd to raise"
            except RuntimeError as e:
                assert "zstd decompression failed" in str(e), e
            assert _time.time() - t0 < 30, "watchdog did not bound the wedged child"
        finally:
            fpr._ZSTD_CLI_TIMEOUT_SEC = saved_timeout
            os.environ["PATH"] = saved_path
            if saved_mod == "MISSING":
                _sys.modules.pop("zstandard", None)
            else:
                _sys.modules["zstandard"] = saved_mod


def test_prefixed_reader_reassembles_stream():
    import io as _io

    payload = bytes(range(256)) * 8
    reader = fpr._PrefixedReader(payload[:4], _io.BytesIO(payload[4:]))
    # Small reads across the prefix boundary and a final read-all must round-trip.
    got = reader.read(2) + reader.read(5) + reader.read()
    assert got == payload


def test_download_shard_isolates_failures():
    import gzip as _gzip
    import subprocess as _sp
    import urllib.error as _ue

    shard = {"arch": "arm", "shard_num": 1, "tsv_url": "https://example/all-query-metrics.tsv"}
    failures = [
        _ue.URLError("connection refused"),
        _sp.TimeoutExpired(cmd="zstd", timeout=120),
        FileNotFoundError("zstd"),  # zstd binary unavailable
        _gzip.BadGzipFile("not a gzip file"),
        RuntimeError("HTTP 403"),
    ]
    original = fpr.download_url
    try:
        with tempfile.TemporaryDirectory() as tmp:
            for exc in failures:
                fpr.download_url = lambda *a, _e=exc, **k: (_ for _ in ()).throw(_e)
                result = fpr.download_shard(shard, tmp)  # must not raise
                assert result[0] is shard and result[1] is None, (exc, result)
                assert "Failed to download" in result[2], (exc, result)
    finally:
        fpr.download_url = original


if __name__ == "__main__":
    test_classification_matches_compare_sh()
    test_summary_counts()
    test_maybe_decompress_handles_plain_gzip_zstd()
    test_stream_to_file_handles_plain_gzip_zstd()
    test_stream_to_file_zstd_cli_fallback()
    test_stream_to_file_zstd_cli_times_out()
    test_prefixed_reader_reassembles_stream()
    test_download_shard_isolates_failures()
    print("All fetch_perf_report tests passed (or skipped).")
