"""
Tests for `report_for` (ci/jobs/scripts/s3_key_lifecycle.py).

The report joins a "No such key" match to the same key's upload and delete lines in the
server logs. Its characteristic failure is silence: an empty report is indistinguishable
from "this key has no lifecycle", so every case below asserts on content, and a key with
nothing to show must say so rather than print nothing.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.s3_key_lifecycle import MAX_EXPANDED_KEYS, report_for

_KEY = "test/hbh/aaaaaaaaaaaaaaaaaaaaaaaaa"
_OTHER = "test/hbh/bbbbbbbbbbbbbbbbbbbbbbbbb"

_MATCH = (
    "2026.09.14 12:00:09.100000 [ 1111 ] {q-1} <Error> executeQuery: Code: 499. "
    "DB::Exception: The specified key does not exist. (S3_ERROR) "
    f"(in query: SELECT * FROM t): while reading key: {_KEY}, from bucket: b"
)


def _logs(tmp_path, key=_KEY, other=_OTHER, match=_MATCH):
    # The lifecycle is deliberately SPLIT across the two log files a stress job leaves
    # behind, so a single-file scan cannot pass.
    (tmp_path / "clickhouse-server.stress.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{q-0}} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_1_1_0/data.bin, key {key}, size 4096\n"
        f"2026.09.14 12:00:02.000000 [ 1002 ] {{}} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_2_2_0/data.bin, key {other}, size 8192\n"
        f"{match}\n",
        encoding="utf-8",
    )
    (tmp_path / "clickhouse-server.final.log").write_text(
        f"2026.09.14 12:00:04.000000 [ 1004 ] {{q-9}} <Debug> deleteFileFromS3: "
        f"Objects with paths [{key}, test/hbh/zzz] were removed from S3\n"
        f"2026.09.14 12:00:06.000000 [ 1006 ] {{}} <Debug> deleteFileFromS3: "
        f"Object with path {other} was removed from S3\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(f"{match}\n", encoding="utf-8")
    return matches


def _group(report, key):
    """The report lines under `--- key: <key>`, up to the next key header."""
    header = f"--- key: {key}"
    assert header in report, report
    body = []
    for line in report[report.index(header) + 1 :]:
        if line.startswith("--- key: "):
            break
        body.append(line)
    return body


def test_upload_and_delete_of_the_matched_key_are_reported(tmp_path):
    # Both events are found, across both log files, and the batch delete matches even
    # though the key is embedded in a comma-separated "[k1, k2]" list.
    body = _group(report_for(_logs(tmp_path), tmp_path), _KEY)

    assert len(body) == 2, body
    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body
    assert any("were removed from S3" in line for line in body), body


def test_the_lifecycle_is_ordered_by_time_not_by_log_file(tmp_path):
    # The upload is in the stress log and the delete that followed it is in the final log,
    # which grep reads first: printed in file order the report would read as a write after
    # a delete.
    body = _group(report_for(_logs(tmp_path), tmp_path), _KEY)

    assert "Writing blob for path" in body[0], body
    assert "were removed from S3" in body[1], body


def test_another_keys_lifecycle_is_not_attributed_to_the_match(tmp_path):
    # `_OTHER` has its own upload and delete lines but is named by no match line.
    report = report_for(_logs(tmp_path), tmp_path)

    assert _OTHER not in "\n".join(report), report
    # The match line itself is not a lifecycle line: it is selected by logger name.
    assert "executeQuery" not in "\n".join(report), report


def test_a_key_with_no_lifecycle_says_so_instead_of_printing_nothing(tmp_path):
    matches = _logs(tmp_path)
    for name in ("clickhouse-server.stress.log", "clickhouse-server.final.log"):
        path = tmp_path / name
        kept = [line for line in path.read_text().splitlines() if _KEY not in line]
        path.write_text("\n".join(kept) + "\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert len(body) == 1, body
    assert body[0].startswith("no lifecycle line found"), body


def test_a_key_absent_from_the_logs_reports_neither_key(tmp_path):
    # Proves the key is what joins the report to the logs, not the message text.
    absent = "test/hbh/ddddddddddddddddddddddddd"
    matches = _logs(tmp_path, match=_MATCH.replace(_KEY, absent))

    report = report_for(matches, tmp_path)

    assert _group(report, absent)[0].startswith("no lifecycle line found"), report
    assert _KEY not in "\n".join(report), report
    assert _OTHER not in "\n".join(report), report


def test_the_key_cap_never_drops_a_key(tmp_path):
    # The match set is unbounded at the stress-test site, so the cap may leave a key
    # unexpanded but must never let it vanish: the largest incident is the one worth
    # attributing.
    keys = [f"test/hbh/k{i:04d}" for i in range(MAX_EXPANDED_KEYS + 50)]
    matches = _logs(tmp_path)
    matches.write_text(
        "\n".join(_MATCH.replace(_KEY, key) for key in keys) + "\n", encoding="utf-8"
    )

    report = report_for(matches, tmp_path)
    headers = [line for line in report if line.startswith("--- key: ")]
    unexpanded = [line for line in report if line.startswith("not expanded:")]

    assert len(headers) == len(keys)
    assert len(unexpanded) == 50
    assert _group(report, keys[MAX_EXPANDED_KEYS]) == [
        f"not expanded: per-report key cap {MAX_EXPANDED_KEYS} reached"
    ]
