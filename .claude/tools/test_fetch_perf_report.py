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

import contextlib
import importlib.util
import io
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


def test_shard_partition_distinguishes_abstained_from_never_run():
    import json as _json

    base = "https://s3.amazonaws.com/clickhouse-test-reports"
    metrics = (
        f"{base}/PRs/1/deadbeef/pr/"
        "performance_comparison_arm_release_master_head_1_6/all-query-metrics.tsv"
    )

    def shard_row(n, status, links):
        return {
            "name": f"Performance Comparison (arm_release, master_head, {n}/6)",
            "status": status,
            "info": "",
            "links": links,
        }

    body = _json.dumps({
        "results": [
            shard_row(1, "SKIPPED", [metrics]),   # abstained: measured, no verdict
            shard_row(2, "SKIPPED", []),          # never ran: no artifacts at all
            shard_row(3, "OK", [metrics]),        # judged normally
        ]
    })

    saved = fpr.fetch_url
    try:
        fpr.fetch_url = lambda url: body
        shards = fpr.get_performance_shards(base, 1, "deadbeef")
    finally:
        fpr.fetch_url = saved

    assert len(shards) == 3, shards
    a, b, c = shards
    assert [s["has_metrics_artifact"] for s in shards] == [True, False, True], shards
    # The published link is used as is; only the shard with no artifact gets the
    # synthesized fallback URL.
    assert a["tsv_url"] == metrics, a
    assert b["tsv_url"].endswith("/all-query-metrics.tsv") and b["tsv_url"] != metrics, b

    assert fpr.shard_abstained(a) and not fpr.shard_never_ran(a)
    assert fpr.shard_never_ran(b) and not fpr.shard_abstained(b)
    assert not fpr.shard_abstained(c) and not fpr.shard_never_ran(c)

    # Mutation control: with the pre-fix predicate (status alone), the abstaining
    # shard is classified as never-run and dropped before download. This arm
    # reddens if the artifact term is ever removed.
    def never_ran_pre_fix(shard):
        return str(shard.get("status", "")).upper() == "SKIPPED"

    assert never_ran_pre_fix(a), a
    assert [s["name"] for s in shards if not never_ran_pre_fix(s)] == [c["name"]]
    assert [s["name"] for s in shards if not fpr.shard_never_ran(s)] == [
        a["name"], c["name"]
    ]


def test_not_judged_is_derived_from_the_exported_bars():
    # A shard that fetched no learned thresholds and *also* failed for an
    # unrelated reason (a report carrying "N errors" is the most common perf
    # failure) keeps FAIL, so the status-based predicate cannot see it. Its
    # exported bars can: every row is inf.
    with tempfile.TemporaryDirectory() as d:
        judged = os.path.join(d, "judged.tsv")
        _write_fixture(judged)

        abstained = os.path.join(d, "abstained.tsv")
        with open(abstained, "w") as f:
            for i, (name, diff, stat, _c, _u) in enumerate(ROWS):
                f.write("\t".join([
                    "amd", "1", "client_time", "1.0", f"{1.0 + diff}",
                    f"{diff}", f"{abs(diff) + 1.0}", f"{stat}",
                    "test_a", str(i), name, "inf", "inf",
                ]) + "\n")

        # A report predating the threshold columns has no bars to read, so it
        # must not be mistaken for an abstention.
        legacy = os.path.join(d, "legacy.tsv")
        with open(legacy, "w") as f:
            with open(judged) as src:
                for line in src:
                    f.write("\t".join(line.rstrip("\n").split("\t")[:11]) + "\n")

        empty = os.path.join(d, "empty.tsv")
        open(empty, "w").close()

        # Only BOTH bars infinite is the sentinel. eqmed.sql divides by the
        # baseline median, so a zero baseline gives an infinite diff, which
        # raises changed_threshold alone; NaN is a missing bar. Neither is an
        # abstention, and perf_api.py partitions them the same way.
        def one_row(name, c_thr, u_thr, stat="0.40"):
            p = os.path.join(d, f"{name}.tsv")
            with open(p, "w") as f:
                f.write("\t".join([
                    "amd", "1", "client_time", "1.0", "1.0", "0.0", "1.0",
                    stat, "test_a", "5", name, c_thr, u_thr,
                ]) + "\n")
            return p

        assert fpr.file_not_judged(abstained)
        assert not fpr.file_not_judged(judged)
        assert not fpr.file_not_judged(legacy)
        assert not fpr.file_not_judged(empty)
        assert not fpr.file_not_judged(one_row("one_bar_inf", "inf", "0.25"))
        assert not fpr.file_not_judged(one_row("other_bar_inf", "0.20", "inf"))
        assert not fpr.file_not_judged(one_row("nan_bars", "nan", "nan"))
        assert not fpr.file_not_judged(one_row("neg_inf", "-inf", "-inf"))
        assert fpr.file_not_judged(one_row("both_inf", "inf", "inf"))

        failed = {
            "name": "Performance Comparison (arm_release, master_head, 1/6)",
            "status": "FAIL", "arch": "arm", "shard_num": 1,
            "has_metrics_artifact": True,
        }
        skipped = {
            "name": "Performance Comparison (arm_release, master_head, 2/6)",
            "status": "SKIPPED", "arch": "arm", "shard_num": 2,
            "has_metrics_artifact": True,
        }
        ok = {
            "name": "Performance Comparison (amd_release, master_head, 3/6)",
            "status": "OK", "arch": "amd", "shard_num": 3,
            "has_metrics_artifact": True,
        }
        # The set main() feeds to the output paths. The status term alone misses
        # the FAIL shard, and that miss is the false clean.
        assert not fpr.shard_abstained(failed), failed
        # The SKIPPED shard is paired with a report predating the bar columns, so
        # only the status term can see it and only the bar term can see the FAIL
        # one: dropping either leaves a shard whose verdict does not exist.
        assert [s["name"] for s in fpr.unjudged_shards(
            [(failed, abstained), (skipped, legacy), (ok, judged)]
        )] == [failed["name"], skipped["name"]]
        # Reversal control: on a judged run the same FAIL shard is not unjudged,
        # so the term did not simply mark every failing shard.
        assert fpr.unjudged_shards([(failed, judged), (ok, judged)]) == []


def test_output_discloses_unjudged_shards():
    # An abstaining shard scores zero on every count, so both output paths would
    # otherwise render it exactly like a shard that measured and found nothing.
    import contextlib as _cl
    import io as _io
    import json as _json

    rows = [
        {"name": "Performance Comparison (arm_release, master_head, 1/6)",
         "faster": 0, "slower": 0, "unstable": 0, "total": 137},
        {"name": "Performance Comparison (arm_release, master_head, 2/6)",
         "faster": 0, "slower": 0, "unstable": 0, "total": 141},
    ]
    abstained, judged = rows[0]["name"], rows[1]["name"]
    # The icon assertions below are only about the icon if no name carries "OK".
    assert "OK" not in abstained and "OK" not in judged

    def capture(fn, *args):
        buf = _io.StringIO()
        with _cl.redirect_stdout(buf):
            fn(*args)
        return buf.getvalue()

    def line_for(text, name):
        hits = [ln for ln in text.splitlines() if name in ln]
        assert len(hits) == 1, (name, hits)
        return hits[0]

    human = capture(fpr.output_human, rows, [], 121189, "client_time", True,
                    {abstained}, set())
    assert "not judged" in line_for(human, abstained)
    assert "OK" not in line_for(human, abstained)
    assert "OK" in line_for(human, judged), line_for(human, judged)
    assert "no changes" in line_for(human, judged)
    assert "No significant performance changes detected." not in human
    assert "not a clean comparison" in human

    # Mutation control: an empty not_judged set is the pre-fix state, and it must
    # bring every claim the fix removed back, so dropping any one of the three
    # output changes reddens this arm.
    pre_human = capture(fpr.output_human, rows, [], 121189, "client_time", True,
                        set(), set())
    assert "OK" in line_for(pre_human, abstained)
    assert "no changes" in line_for(pre_human, abstained)
    assert "No significant performance changes detected." in pre_human
    assert human != pre_human

    js = _json.loads(capture(fpr.output_json, rows, [], 121189, "deadbeef",
                             "client_time", {abstained}, set()))
    pre_js = _json.loads(capture(fpr.output_json, rows, [], 121189, "deadbeef",
                                 "client_time", set(), set()))
    assert [s["not_judged"] for s in js["shards"]] == [True, False], js
    assert [s["not_judged"] for s in pre_js["shards"]] == [False, False], pre_js
    assert js != pre_js


def test_detail_rows_disclose_unjudged_shards():
    # Under --all the display filter is empty, so an abstaining shard's own rows
    # reach the detail list. Its inf bars leave is_changed and is_unstable false,
    # which is the shape that used to be filed under "unchanged" -- a verdict
    # those rows never got -- directly below that shard's own "not judged" line.
    import contextlib as _cl
    import io as _io
    import json as _json

    SEP = "-" * 90

    def detail_row(shard_num, test):
        # diff is large while both flags are false: exactly what inf bars produce
        # (the bars, not stat_threshold, are what abstention sets to inf).
        return {
            "test": test, "query_index": 0,
            "arch": "arm", "shard": shard_num,
            "old": 1.0, "new": 2.0, "diff": 1.0,
            "times_change": 2.0, "stat_threshold": 0.05,
            "is_changed": 0, "is_unstable": 0,
            "direction": "slower", "query": f"SELECT {test}",
        }

    abstaining_row = detail_row(1, "test_abstained")
    detail_rows = [abstaining_row, detail_row(2, "test_judged")]
    summary = [
        {"name": "Performance Comparison (arm_release, master_head, 1/6)",
         "faster": 0, "slower": 0, "unstable": 0, "total": 1},
        {"name": "Performance Comparison (arm_release, master_head, 2/6)",
         "faster": 0, "slower": 0, "unstable": 0, "total": 1},
    ]
    abstained_name = summary[0]["name"]
    keys = {("arm", 1)}

    def capture(fn, *args):
        buf = _io.StringIO()
        with _cl.redirect_stdout(buf):
            fn(*args)
        return buf.getvalue()

    def section_of(text, needle):
        """Heading of the section the one line containing `needle` sits in.

        Every section heading is printed sandwiched between two dashed rules,
        which no other line is.
        """
        lines = text.splitlines()
        heading, found = "(preamble)", None
        for i, ln in enumerate(lines):
            if 0 < i < len(lines) - 1 and lines[i - 1] == SEP and lines[i + 1] == SEP:
                heading = ln
            if needle in ln:
                assert found is None, (needle, found, ln)
                found = heading
        assert found is not None, (needle, text)
        return found

    # A1 -- mixed run: the unjudged row is quarantined and, with detail rows
    # present, the disclosure sentence still prints.
    assert detail_rows, "the bug needs a non-empty detail list"
    human = capture(fpr.output_human, summary, detail_rows, 121189, "client_time",
                    True, {abstained_name}, keys)
    assert section_of(human, "test_abstained").startswith("NOT JUDGED"), human
    assert section_of(human, "test_judged").startswith("ALL QUERIES"), human
    assert "ALL QUERIES (1 unchanged)" in human, human
    assert "not a clean comparison" in human, human

    # A2 -- reversal control: without the key set both rows are labelled
    # "unchanged" and the sentence is gone, so dropping either fix reddens here.
    pre_human = capture(fpr.output_human, summary, detail_rows, 121189,
                        "client_time", True, set(), set())
    assert "NOT JUDGED" not in pre_human, pre_human
    assert "ALL QUERIES (2 unchanged)" in pre_human, pre_human
    assert section_of(pre_human, "test_abstained").startswith("ALL QUERIES"), pre_human
    assert "not a clean comparison" not in pre_human, pre_human
    assert human != pre_human

    # A3 -- the key built from a real shard dict must equal the key built from a
    # detail row for that shard: a silent str/int mismatch would make the
    # partition inert while A1/A2 still passed on hand-made data.
    base = "https://s3.amazonaws.com/clickhouse-test-reports"
    metrics = (
        f"{base}/PRs/1/deadbeef/pr/"
        "performance_comparison_arm_release_master_head_1_6/all-query-metrics.tsv"
    )
    body = _json.dumps({"results": [{
        "name": abstained_name, "status": "SKIPPED", "info": "",
        "links": [metrics],
    }]})
    saved = fpr.fetch_url
    try:
        fpr.fetch_url = lambda url: body
        shard = fpr.get_performance_shards(base, 1, "deadbeef")[0]
    finally:
        fpr.fetch_url = saved
    assert fpr.shard_abstained(shard), shard
    assert isinstance(shard["arch"], str) and isinstance(shard["shard_num"], int), shard
    assert (shard["arch"], shard["shard_num"]) == (
        abstaining_row["arch"], abstaining_row["shard"]
    ), (shard, abstaining_row)
    assert (abstaining_row["arch"], abstaining_row["shard"]) in keys

    # A4 -- json: the marker follows the same key set, and the row's own fields
    # survive the rewrite.
    js = _json.loads(capture(fpr.output_json, summary, detail_rows, 121189,
                             "deadbeef", "client_time", {abstained_name}, keys))
    pre_js = _json.loads(capture(fpr.output_json, summary, detail_rows, 121189,
                                 "deadbeef", "client_time", set(), set()))
    by_test = {q["test"]: q for q in js["queries"]}
    assert by_test["test_abstained"]["not_judged"] is True, js
    assert by_test["test_judged"]["not_judged"] is False, js
    assert by_test["test_abstained"]["diff"] == 1.0, js
    assert [q["not_judged"] for q in pre_js["queries"]] == [False, False], pre_js
    assert js != pre_js

    # A5 -- a genuinely clean run keeps its own sentence.
    clean = capture(fpr.output_human, summary, [], 121189, "client_time", True,
                    set(), set())
    assert "No significant performance changes detected." in clean, clean
    assert "NOT JUDGED" not in clean and "not a clean comparison" not in clean, clean


def test_tsv_rows_carry_the_abstention_marker():
    # --tsv has no shard-level summary line, so an abstaining shard's rows shipped
    # is_changed=0 and is_unstable=0 and nothing else: each row asserting a verdict
    # it never got. The marker column is what lets the row say otherwise. This arm
    # asserts on the SQL text, so it needs no clickhouse binary.
    args = _args()
    data_path = "/tmp/never-read-only-interpolated.tsv"

    def column_of(sql):
        """Text between the last stable column and FROM: the new column alone."""
        assert "query_display_name AS query" in sql, sql
        tail = sql.split("query_display_name AS query", 1)[1]
        assert "FROM filtered" in tail, sql
        return tail.split("FROM filtered", 1)[0]

    tsv = fpr.build_detail_sql(args, data_path, has_thresholds=True,
                               fmt="TabSeparatedWithNames",
                               not_judged_keys={("arm", 1)})
    assert tsv.count("AS not_judged") == 1, tsv
    assert "FORMAT TabSeparatedWithNames" in tsv, tsv
    # The marker follows query_display_name, so the published column order is
    # unchanged and a positional reader of the older columns keeps working.
    col = column_of(tsv)
    assert "AS not_judged" in col, tsv
    assert "arch = 'arm'" in col and "shard_num = 1" in col, col

    # Reversal control: the marker is absent when no shard abstained, so this arm
    # cannot pass vacuously on a build that always projects the column.
    plain_tsv = fpr.build_detail_sql(args, data_path, has_thresholds=True,
                                     fmt="TabSeparatedWithNames",
                                     not_judged_keys=None)
    assert "not_judged" not in plain_tsv, plain_tsv
    assert plain_tsv != tsv

    # A run where no shard abstained passes an empty key set, which is falsy and so
    # reaches this same form: the SQL must then be exactly what it was before the
    # marker existed, for every output mode.
    js = fpr.build_detail_sql(args, data_path, has_thresholds=True)
    assert "not_judged" not in js, js
    assert "FORMAT JSONEachRow" in js, js

    # Two abstaining shards must both be matched. With AND between the disjuncts
    # the predicate is unsatisfiable and every row reads "judged" again.
    two = fpr.build_detail_sql(args, data_path, has_thresholds=True,
                               fmt="TabSeparatedWithNames",
                               not_judged_keys={("arm", 1), ("amd", 2)})
    col2 = column_of(two)
    assert "(arch = 'arm' AND shard_num = 1)" in col2, col2
    assert "(arch = 'amd' AND shard_num = 2)" in col2, col2
    assert col2.count(" OR ") == 1, col2


def _load_perf_api():
    """The perf-comparison skill helper, the documented consumer of --tsv output."""
    path = os.path.join(_HERE, "..", "skills", "perf-comparison", "scripts", "perf_api.py")
    spec = importlib.util.spec_from_file_location("perf_api", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_tsv_output_reaches_the_consumer_as_unjudged():
    """End to end over the real output_tsv in its DEFAULT mode: an unjudged shard flags
    nothing, so the flag-only filter would drop exactly the rows the marker labels, and
    the consumer would count zero. Judged shards keep their normal filtering."""
    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    tmpdir = tempfile.mkdtemp(prefix="test_perf_wire_")
    try:
        data_path = os.path.join(tmpdir, "all.tsv")
        with open(data_path, "w") as f:
            for arch, shard, name, diff, c_thr, u_thr in [
                ("arm", "1", "abstained_big", 1.0, "inf", "inf"),
                ("arm", "1", "abstained_steady", 0.0, "inf", "inf"),
                ("amd", "2", "judged_changed", 0.30, "0.20", "0.25"),
                ("amd", "2", "judged_steady", 0.00, "0.20", "0.25"),
            ]:
                f.write("\t".join([
                    arch, shard, "client_time", "1.0", f"{1.0 + diff}", f"{diff}",
                    f"{abs(diff) + 1.0}", "0.05", "test_a", "0", name, c_thr, u_thr,
                ]) + "\n")

        def emitted(not_judged_keys):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                fpr.output_tsv(_args(show_all=False), data_path, True, not_judged_keys)
            return buf.getvalue()

        out = emitted({("arm", 1)})
        rows = [line.split("\t") for line in out.strip().split("\n")]
        header, data = rows[0], rows[1:]
        marked = {r[header.index("query")]: r[header.index("not_judged")] for r in data}
        assert marked.get("abstained_big") == "1", out
        assert marked.get("abstained_steady") == "1", out
        assert marked.get("judged_changed") == "0", out
        # The judged shard's steady row is still filtered out, so the term did not
        # turn the default mode into --all.
        assert "judged_steady" not in marked, out

        tsv_path = os.path.join(tmpdir, "emitted.tsv")
        with open(tsv_path, "w") as f:
            f.write(out)
        perf_api = _load_perf_api()
        buckets = {r["queryDisplayName"]: r["bucket"] for r in perf_api.parse_perf_tsv([tsv_path])}
        assert buckets["abstained_big"] == "not-judged", buckets
        assert buckets["abstained_steady"] == "not-judged", buckets
        assert buckets["judged_changed"] == "changed", buckets

        # Reversal control: with no abstaining shard discovered, both unjudged rows are
        # absent from the default output, which is what the marker alone could not fix.
        plain = emitted(None)
        assert "abstained_big" not in plain and "not_judged" not in plain, plain
        assert "judged_changed" in plain, plain
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_default_non_tsv_paths_emit_the_unjudged_rows():
    """End to end through the real main(), which is where the --json and default human
    paths choose their detail filter. Both are documented without --all
    (.claude/CLAUDE.md), and an unjudged shard flags nothing, so a flag-only filter
    drops every row those paths promise to label: --json omits the shard's queries
    entirely while the shard-level n/j line still prints. Only main() wires this, so no
    test of output_json / output_human can reach it."""
    import json as _json
    import sys as _sys

    if shutil.which("clickhouse") is None:
        print("SKIP: clickhouse binary not available")
        return

    def shards(abstained):
        return [
            {"name": "Performance Comparison (arm_release, master_head, 1/6)",
             "arch": "arm", "shard_num": 1,
             "status": "SKIPPED" if abstained else "FAILURE",
             "has_metrics_artifact": True, "info": "",
             "tsv_url": "https://example.invalid/arm.tsv"},
            {"name": "Performance Comparison (amd_release, master_head, 2/6)",
             "arch": "amd", "shard_num": 2, "status": "FAILURE",
             "has_metrics_artifact": True, "info": "",
             "tsv_url": "https://example.invalid/amd.tsv"},
        ]

    def rows_of(shard, abstained):
        """(query name, diff, changed_threshold, unstable_threshold) per shard.

        Only an abstention makes the exported bars infinite, so the judged control
        gets finite ones. Reusing inf bars for a shard the tool treats as judged
        would assert that an unjudged row may be reported as unchanged, which is the
        very claim this marker exists to refute.
        """
        if (shard["arch"], shard["shard_num"]) != ("arm", 1):
            return [("amd2_changed", 0.30, "0.20", "0.25"),
                    ("amd2_steady", 0.00, "0.20", "0.25")]
        bars = ("inf", "inf") if abstained else ("0.20", "0.25")
        return [("arm1_big", 1.00) + bars, ("arm1_steady", 0.00) + bars]

    def run(extra_argv, abstained=True):
        """Real main(): only shard discovery and the download are stubbed."""
        def fake_download(shard, tmpdir):
            dest = os.path.join(tmpdir, f"{shard['arch']}_{shard['shard_num']}.tsv")
            with open(dest, "w") as f:
                for name, diff, c_thr, u_thr in rows_of(shard, abstained):
                    f.write("\t".join([
                        shard["arch"], str(shard["shard_num"]), "client_time",
                        "1.0", f"{1.0 + diff}", f"{diff}", f"{abs(diff) + 1.0}",
                        "0.05", "test_a", "0", name, c_thr, u_thr,
                    ]) + "\n")
            return shard, dest, None

        saved = (_sys.argv, fpr.get_performance_shards, fpr.download_shard)
        out = io.StringIO()
        try:
            _sys.argv = ["fetch_perf_report.py",
                         "https://s3.amazonaws.com/x/json.html?PR=1&sha=deadbeef"] + extra_argv
            fpr.get_performance_shards = lambda base, pr, sha: shards(abstained)
            fpr.download_shard = fake_download
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(io.StringIO()):
                fpr.main()
        finally:
            _sys.argv, fpr.get_performance_shards, fpr.download_shard = saved
        return out.getvalue()

    # B1 -- default human mode: the shard line and the promised rows must agree.
    human = run([])
    assert "[ n/j]" in human, human
    assert "NOT JUDGED (2 queries" in human, human
    assert "CHANGES IN PERFORMANCE" in human, human
    # Proves this is the default mode and not --all: the judged shard's steady row is
    # still filtered, so the widened filter did not become a blanket "show everything".
    assert "ALL QUERIES" not in human, human

    # B2 -- default --json: the rows the shard flag refers to must be in `queries`.
    js = _json.loads(run(["--json"]))
    arm1 = [q for q in js["queries"] if (q["arch"], q["shard"]) == ("arm", 1)]
    assert sorted(q["query"] for q in arm1) == ["arm1_big", "arm1_steady"], js
    assert all(q["not_judged"] is True for q in arm1), js
    # The SQL marker is an integer; output_json's own bool must win, or a consumer
    # testing `is True` silently sees every row as judged.
    judged = [q for q in js["queries"] if q["query"] == "amd2_changed"]
    assert judged and judged[0]["not_judged"] is False, js
    assert "amd2_steady" not in [q["query"] for q in js["queries"]], js
    assert [s["not_judged"] for s in js["shards"]] == [False, True], js

    # B3 -- reversal control: the same two shards with finite bars on arm/1, so nothing
    # abstained. Both modes must be exactly what they were before the marker existed,
    # and arm/1's rows must now be classified normally rather than quarantined.
    plain_human = run([], abstained=False)
    assert "NOT JUDGED" not in plain_human and "[ n/j]" not in plain_human, plain_human
    assert "arm1_big" in plain_human, plain_human
    assert "ALL QUERIES" not in plain_human, plain_human
    plain_js = _json.loads(run(["--json"], abstained=False))
    assert sorted(q["query"] for q in plain_js["queries"]) == ["amd2_changed", "arm1_big"], plain_js
    assert not any(q["not_judged"] for q in plain_js["queries"]), plain_js


if __name__ == "__main__":
    test_classification_matches_compare_sh()
    test_summary_counts()
    test_maybe_decompress_handles_plain_gzip_zstd()
    test_stream_to_file_handles_plain_gzip_zstd()
    test_stream_to_file_zstd_cli_fallback()
    test_stream_to_file_zstd_cli_times_out()
    test_prefixed_reader_reassembles_stream()
    test_download_shard_isolates_failures()
    test_shard_partition_distinguishes_abstained_from_never_run()
    test_not_judged_is_derived_from_the_exported_bars()
    test_output_discloses_unjudged_shards()
    test_detail_rows_disclose_unjudged_shards()
    test_tsv_rows_carry_the_abstention_marker()
    test_default_tsv_output_reaches_the_consumer_as_unjudged()
    test_default_non_tsv_paths_emit_the_unjudged_rows()
    print("All fetch_perf_report tests passed (or skipped).")
