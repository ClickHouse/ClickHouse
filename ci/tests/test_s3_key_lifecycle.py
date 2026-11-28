"""
Tests for `report_for` (ci/jobs/scripts/s3_key_lifecycle.py).

The report joins a "No such key" match to the same key's upload and delete lines in the
server logs. Its characteristic failure is silence: an empty report is indistinguishable
from "this key has no lifecycle", so every case below asserts on content, and a key with
nothing to show must say so rather than print nothing.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.s3_key_lifecycle import MAX_EXPANDED_KEYS, MAX_LINES_PER_KEY, report_for

_KEY = "test/hbh/aaaaaaaaaaaaaaaaaaaaaaaaa"
_OTHER = "test/hbh/bbbbbbbbbbbbbbbbbbbbbbbbb"

# Field order as executeQuery assembles it: the query id, then the level, then the exception
# text whose tail is the suffix ReadBufferFromS3 appended, and only then the query.
_MATCH = (
    "2026.09.14 12:00:09.100000 [ 1111 ] {q-1} <Error> executeQuery: Code: 499. "
    "DB::Exception: The specified key does not exist. This error happened for S3 disk: "
    f"while reading key: {_KEY}, from bucket: b. (S3_ERROR) (from 1.2.3.4:5678) "
    "(in query: SELECT * FROM t)"
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


def test_two_matched_keys_each_get_their_own_lifecycle(tmp_path):
    # The report is per-key, so the dispatch over every group has to be proven and not just the
    # single-key path: a collector that stopped after the first key passes every case above.
    matches = _logs(tmp_path)
    matches.write_text(
        f"{_MATCH}\n{_MATCH.replace(_KEY, _OTHER)}\n", encoding="utf-8"
    )

    report = report_for(matches, tmp_path)
    first = _group(report, _KEY)
    second = _group(report, _OTHER)

    assert len(first) == 2, first
    assert all(_OTHER not in line for line in first), first
    assert any("all_1_1_0/data.bin" in line for line in first), first
    assert any("were removed from S3" in line for line in first), first

    assert len(second) == 2, second
    assert all(_KEY not in line for line in second), second
    assert any("all_2_2_0/data.bin" in line for line in second), second
    assert any("was removed from S3" in line for line in second), second


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


def test_a_reworded_lifecycle_message_is_still_reported(tmp_path):
    # The property the report is built on: lines are selected by logger name, so the report
    # survives a reworded message. Reddens as soon as selection looks at the wording.
    (tmp_path / "clickhouse-server.stress.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{q-0}} <Test> DiskObjectStorageTransaction: "
        f"Uploading object for path all_1_1_0/data.bin, key {_KEY}, bytes 4096\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(f"{_MATCH}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert len(body) == 1, body
    assert "Uploading object for path all_1_1_0/data.bin" in body[0], body


def test_a_key_nested_in_a_longer_key_is_not_attributed_to_it(tmp_path):
    # A key can occur inside a longer key: the plain families key an object by its path, and
    # nothing keeps one path from nesting another. Under a bare substring test the shorter
    # key's group shows the longer key's upload and delete.
    part = "test/hbh/store/abc/all_1_1_0"
    below = f"{part}/data.bin"
    (tmp_path / "clickhouse-server.final.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{q-0}} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_1_1_0/data.bin, key {below}, size 4096\n"
        f"2026.09.14 12:00:02.000000 [ 1002 ] {{}} <Debug> deleteFileFromS3: "
        f"Objects with paths [{below}] were removed from S3\n"
        f"2026.09.14 12:00:03.000000 [ 1003 ] {{}} <Debug> deleteFileFromS3: "
        f"Object with path {part} was removed from S3\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(_MATCH.replace(_KEY, part) + "\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), part)

    assert len(body) == 1, body
    assert f"Object with path {part} was removed" in body[0], body
    assert "data.bin" not in "\n".join(body), body


def test_a_key_diverging_on_an_escaped_character_is_not_reported_as_its_own(tmp_path):
    # Sibling paths `x` and `x.y` become the keys `x` and `x%2Ey`: an object key is the caller
    # path put through `escapeForFileName`, which emits `%XX` for every byte outside
    # [A-Za-z0-9_]. `%` is a key character no alphabet enumerates, so the boundary has to come
    # from the separators the messages use, and the longer key's lines have to arrive under the
    # substring note rather than as this key's own lifecycle.
    part = "test/hbh/store/abc/x"
    longer = f"{part}%2Ey/data.bin"
    (tmp_path / "clickhouse-server.final.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{q-0}} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_1_1_0/data.bin, key {longer}, size 1\n"
        f"2026.09.14 12:00:02.000000 [ 1002 ] {{}} <Debug> deleteFileFromS3: "
        f"Objects with paths [{longer}] were removed from S3\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(_MATCH.replace(_KEY, part) + "\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), part)

    # The note is the whole difference between honest and misleading here: the two lines on
    # their own read as this key's upload and delete.
    assert len(body) == 3, body
    assert body[0].startswith("matched as a substring only"), body
    assert any("Writing blob for path" in line for line in body[1:]), body
    assert any("were removed from S3" in line for line in body[1:]), body


def test_a_signal_killed_scan_raises_instead_of_reporting_no_lifecycle(tmp_path, monkeypatch):
    # The scan runs at the end of a stress job over the largest log set CI produces, where an
    # OOM kill and the runner's TERM are the ordinary deaths. A signal leaves `Popen.wait()` a
    # negative status and the output empty, which is exactly what a key with no lifecycle looks
    # like, so it has to raise: the caller turns a raise into its "collection FAILED" marker.
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_grep = fake_bin / "grep"
    # The key list is drained first, so the death is the signal and not a broken pipe.
    fake_grep.write_text("#!/bin/sh\ncat > /dev/null\nkill -TERM $$\n", encoding="utf-8")
    fake_grep.chmod(0o755)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")

    matches = _logs(tmp_path)

    with pytest.raises(RuntimeError, match=r"grep exited -\d+"):
        report_for(matches, tmp_path)


def test_matches_with_no_extractable_key_say_so_instead_of_printing_nothing(tmp_path):
    # A 499 raised outside ReadBufferFromS3 carries no key, so no group can be built. An
    # empty report would read as "these matches have no lifecycle", which is not the finding.
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(
        "2026.09.14 12:00:09.100000 [ 1111 ] {q-1} <Error> executeQuery: Code: 499. "
        "DB::Exception: The specified key does not exist. (S3_ERROR) (in query: SELECT 1)\n",
        encoding="utf-8",
    )

    report = report_for(matches, tmp_path)

    assert len(report) == 1, report
    assert report[0].startswith("--- no S3 key found in the 1 match line(s) above"), report


def test_a_key_holding_a_space_or_a_comma_gets_its_lifecycle(tmp_path):
    # `s3()` takes its key from the percent-decoded URL path, so "%20" reaches ReadBufferFromS3
    # as a space, and nothing rejects a comma either. Next to a key that parses, such a key gets
    # no header at all, so the report reads as complete while omitting a failing read.
    key = "dir with space,comma/file.txt"
    matches = _logs(tmp_path, key=key)
    matches.write_text(
        f"{_MATCH.replace(_KEY, key)}\n{_MATCH.replace(_KEY, _OTHER)}\n", encoding="utf-8"
    )

    report = report_for(matches, tmp_path)
    body = _group(report, key)

    assert [line for line in report if line.startswith("--- key: ")] == [
        f"--- key: {key}",
        f"--- key: {_OTHER}",
    ], report
    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body
    assert any("were removed from S3" in line for line in body), body


def test_a_key_spelling_the_delimiter_itself_keeps_its_lifecycle(tmp_path):
    # The suffix is the only delimiter the emitter writes, so a key spelling it too is cut at its
    # first occurrence and reported under that prefix. What may not degrade is the history: the
    # prefix is still delimited in the emitter's own lines, so the object's events stay attributed.
    key = "weird, from bucket: x/file.txt"
    matches = _logs(tmp_path, key=key)
    matches.write_text(f"{_MATCH.replace(_KEY, key)}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), "weird")

    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body
    assert any("were removed from S3" in line for line in body), body


def test_a_key_named_without_a_delimiter_is_reported_as_a_substring_match(tmp_path):
    # The delimited-occurrence test must not be able to empty a group on its own: a message
    # that qualifies the key (here with the bucket) still carries the object's lifecycle, so
    # the line is reported under a note saying how it was matched.
    (tmp_path / "clickhouse-server.final.log").write_text(
        f"2026.09.14 12:00:04.000000 [ 1004 ] {{}} <Debug> deleteFileFromS3: "
        f"Object with path b/{_KEY} was removed from S3\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(f"{_MATCH}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert len(body) == 2, body
    assert body[0].startswith("matched as a substring only"), body
    assert f"Object with path b/{_KEY} was removed" in body[1], body


def test_an_empty_match_set_reports_absolutely_nothing(tmp_path):
    # The caller's PASS verdict is that the file this report is appended to stayed empty, so
    # every job without a "No such key" error runs this path: one stray line turns CI red.
    _logs(tmp_path)
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text("", encoding="utf-8")

    assert report_for(matches, tmp_path) == []


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


def test_keys_named_by_the_failing_query_text_are_not_treated_as_matched_keys(tmp_path):
    # A query may name anything, including this suffix. Counting its text as keys spends the
    # report's key cap on strings no read touched, so a later match line's real key is the
    # one that ends up unexpanded.
    spoof = " ".join(
        f"while reading key: spoof/k{i:04d}" for i in range(MAX_EXPANDED_KEYS + 50)
    )
    poisoned = _MATCH.replace("SELECT * FROM t", f"SELECT '{spoof}'")
    matches = _logs(tmp_path)
    matches.write_text(
        f"{poisoned}\n{_MATCH.replace(_KEY, _OTHER)}\n", encoding="utf-8"
    )

    report = report_for(matches, tmp_path)

    assert not any("spoof/" in line for line in report), report
    # The second match line's key is real and must still be expanded, not capped out.
    assert _group(report, _OTHER) != [
        f"not expanded: per-report key cap {MAX_EXPANDED_KEYS} reached"
    ], report


@pytest.mark.parametrize(
    "text",
    [
        "SELECT 'deleteFileFromS3'",
        # A whole level and logger slot: the spelling a name-anywhere filter still admits.
        f"SELECT '<Debug> deleteFileFromS3: Object with path {_KEY} was removed from S3'",
    ],
)
def test_a_lifecycle_logger_named_by_the_query_text_is_not_a_lifecycle_line(tmp_path, text):
    # The logger is a field, not a substring: a query that merely mentions one, in whatever
    # spelling, must not make its own failure line read as this object's history.
    quoted = _MATCH.replace("SELECT * FROM t", text)
    matches = _logs(tmp_path)
    matches.write_text(f"{quoted}\n", encoding="utf-8")
    # The failure line is in the logs too, which is how grep reaches it. Repeated past the
    # per-key cap and put before the upload line, because a selected failure line does not
    # merely add noise: it holds the slot the object's own history needs.
    (tmp_path / "clickhouse-server.stress.log").write_text(
        f"{quoted}\n" * (MAX_LINES_PER_KEY + 1)
        + "2026.09.14 12:00:01.000000 [ 1001 ] {q-0} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_1_1_0/data.bin, key {_KEY}, size 4096\n",
        encoding="utf-8",
    )

    body = _group(report_for(matches, tmp_path), _KEY)

    assert not any("executeQuery" in line for line in body), body
    assert any("Writing blob for path" in line for line in body), body


@pytest.mark.parametrize("query_id", ["has}brace", "holds <Debug> a level"])
def test_a_query_id_holding_a_brace_still_gets_its_lifecycle(tmp_path, query_id):
    # A client may set any query id and the formatter writes it unescaped, so the fields
    # before the logger cannot be parsed. This is the silent direction: an upload line is
    # reported whatever its id spells, because the formatter's own "} " closes that field.
    matches = _logs(tmp_path)
    # Written after _logs, which lays down both log files: the hostile id is the whole point.
    (tmp_path / "clickhouse-server.stress.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{{query_id}}} <Test> DiskObjectStorageTransaction: "
        f"Writing blob for path all_1_1_0/data.bin, key {_KEY}, size 4096\n",
        encoding="utf-8",
    )
    (tmp_path / "clickhouse-server.final.log").write_text("", encoding="utf-8")
    matches.write_text(f"{_MATCH}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body


def test_a_query_id_naming_a_key_does_not_displace_the_matched_one(tmp_path):
    # The id is as untrusted as the query text, and the formatter writes it before the level,
    # so it can hold complete replicas of the suffix - tail included - ahead of the real one,
    # behind a level slot of its own so that reading from the first one starts inside the id.
    ids = " ".join(
        f"while reading key: spoof/k{i:04d}, from bucket: b"
        for i in range(MAX_EXPANDED_KEYS + 5)
    )
    poisoned = _MATCH.replace("{q-1}", f"{{qid <Debug> {ids}}}")
    matches = _logs(tmp_path)
    matches.write_text(f"{poisoned}\n", encoding="utf-8")

    report = report_for(matches, tmp_path)

    assert not any("spoof/" in line for line in report), report
    body = _group(report, _KEY)
    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body


def test_one_match_lines_keys_do_not_crowd_out_another_lines_key(tmp_path):
    # A query that spells the whole suffix out in its own text really does name those keys on
    # its line, and can name more of them than the report expands. Spending the cap on one
    # line before the next is reached is what leaves the following failure unexplained.
    spoof = " ".join(
        f"while reading key: spoof/k{i:04d}, from bucket: b"
        for i in range(MAX_EXPANDED_KEYS + 5)
    )
    poisoned = _MATCH.replace("SELECT * FROM t", f"SELECT '{spoof}'")
    matches = _logs(tmp_path)
    matches.write_text(f"{poisoned}\n{_MATCH.replace(_KEY, _OTHER)}\n", encoding="utf-8")

    report = report_for(matches, tmp_path)
    poisoned_line = _group(report, _KEY)
    next_line = _group(report, _OTHER)

    # The read's own key is the first one its message names, so it survives its own query text.
    assert any("all_1_1_0/data.bin" in line for line in poisoned_line), poisoned_line
    assert any("all_2_2_0/data.bin" in line for line in next_line), next_line
    assert any("was removed from S3" in line for line in next_line), next_line


def test_a_delete_of_another_object_is_not_attributed_by_the_query_id(tmp_path):
    # A genuine delete, of a different object, run under a query id that happens to hold the
    # matched key. Putting it in this object's history is worse than reporting no history at
    # all: it says the object was removed at a time nothing removed it.
    matches = _logs(tmp_path)
    # Written after _logs, which lays down both log files: the hostile id is the whole point.
    (tmp_path / "clickhouse-server.stress.log").write_text(
        f"2026.09.14 12:00:01.000000 [ 1001 ] {{q {_KEY} tail}} <Debug> deleteFileFromS3: "
        f"Object with path {_OTHER} was removed from S3\n",
        encoding="utf-8",
    )
    (tmp_path / "clickhouse-server.final.log").write_text("", encoding="utf-8")
    matches.write_text(f"{_MATCH}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert len(body) == 1, body
    assert body[0].startswith("no lifecycle line found"), body


def test_a_match_line_with_no_level_slot_still_names_its_key(tmp_path):
    # An exception can be several lines long and only its first line carries the prefix
    # fields. A shape this parser cannot dissect must fall back to reading the whole line,
    # because a continuation line's key is as real as any.
    matches = _logs(tmp_path)
    matches.write_text(
        f"Received from localhost:9000. DB::Exception: while reading key: {_KEY}, "
        "from bucket: b.\n",
        encoding="utf-8",
    )

    body = _group(report_for(matches, tmp_path), _KEY)

    assert any("Writing blob for path all_1_1_0/data.bin" in line for line in body), body


def test_the_per_key_line_cap_says_how_many_lines_it_omitted(tmp_path):
    # The per-key cap is the other half of bounding the scan: it may shorten one key's history,
    # but a shortened history that does not say so reads as a complete one.
    extra = 12
    (tmp_path / "clickhouse-server.final.log").write_text(
        "\n".join(
            f"2026.09.14 12:00:{i % 60:02d}.000000 [ 1001 ] {{}} <Debug> deleteFileFromS3: "
            f"Object with path {_KEY} was removed from S3"
            for i in range(MAX_LINES_PER_KEY + extra)
        )
        + "\n",
        encoding="utf-8",
    )
    matches = tmp_path / "no_such_key_errors.txt"
    matches.write_text(f"{_MATCH}\n", encoding="utf-8")

    body = _group(report_for(matches, tmp_path), _KEY)

    assert len(body) == MAX_LINES_PER_KEY + 1, len(body)
    assert body[-1] == (
        f"... {extra} more lifecycle line(s) omitted (per-key line cap {MAX_LINES_PER_KEY})"
    ), body[-1]
