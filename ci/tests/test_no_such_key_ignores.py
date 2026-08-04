"""
Tests for the "Lost s3 keys" / "S3_ERROR No such key thrown" log checks of the
functional tests jobs (`ci.jobs.scripts.clickhouse_proc.no_such_key_check_command`).

The scan is line oriented, while a ClickHouse exception log entry spans many lines:
the message, the stack trace, and a trailing `(version …) (…)` line carrying whatever
`addMessage` appended. An entry printed by an ignored buffer therefore escapes its own
ignore whenever that trailing line repeats the nested `Code: 499` text - which is how a
best-effort cache-warm read of a blob deleted with its part turned into two red checks
with every test green. Every ignored substring gets a case checking that it suppresses a
line naming it and nothing else; a genuine lost key must still be reported.
"""

import os
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.clickhouse_proc import (
    NO_SUCH_KEY_IGNORES,
    no_such_key_check_command,
)

# Verbatim from the failing job (clickhouse-private#67155, `Stateless tests
# (amd_asan_ubsan, distributed cache, meta in keeper, s3 storage, parallel, 1/2)`),
# shortened in the middle: the trailing line of a `<Debug> ReadBufferFromDistributedCache`
# entry, naming the cache warmer as the caller.
CACHE_WARM_TRAILING_LINE = (
    " (version 26.8.1.1) (File name: test/ffx/avmoqhjkisdnuurkyppycsnrkkgjj, local file name: "
    "data/5adcf7ee-55fc-4600-b251-c3c7714beaec/all_1_1_0/columns_substreams.txt, file offset: 0/73, "
    "last error: Code: 900. DB::Exception: Received error from distributed cache: Code: 499. "
    "DB::Exception: The specified key does not exist. This error happened for S3 disk: while reading "
    "key: test/ffx/avmoqhjkisdnuurkyppycsnrkkgjj, from bucket: test: Cache info: file segments: 1 "
    "(front: File segment: [0, 72], state: DOWNLOADING, downloaded size: 0, downloader id: "
    "None:cache-warm:10623:3931, caller id: None:cache-warm:10623:3931, background download: 1)."
)

# A key that is really gone under a query - what the checks exist to catch.
GENUINE_LOST_KEY_LINE = (
    "2026.08.04 07:21:57.003116 [ 10623 ] {b6e0f6c5-1d0e-4f1d-9b02-2f5f0d4a1c77} <Error> executeQuery: "
    "Code: 499. DB::Exception: The specified key does not exist. This error happened for S3 disk: "
    "while reading key: test/ffx/avmoqhjkisdnuurkyppycsnrkkgjj, from bucket: test. (S3_ERROR) "
    "(version 26.8.1.1) (from [::1]:50056) (in query: SELECT * FROM t)"
)

UNRELATED_LINE = "2026.08.04 07:21:57.003116 [ 10623 ] {} <Debug> executeQuery: Read 1 rows"


def run_check(tmp_path, lines):
    (tmp_path / "clickhouse-server.log").write_text("\n".join(lines) + "\n")
    return subprocess.run(
        no_such_key_check_command(tmp_path),
        shell=True,
        capture_output=True,
        text=True,
    ).returncode


def test_genuine_lost_key_is_reported(tmp_path):
    assert run_check(tmp_path, [UNRELATED_LINE, GENUINE_LOST_KEY_LINE]) != 0


def test_clean_log_passes(tmp_path):
    assert run_check(tmp_path, [UNRELATED_LINE]) == 0


def test_cache_warm_trailing_line_is_ignored(tmp_path):
    assert run_check(tmp_path, [CACHE_WARM_TRAILING_LINE]) == 0


def test_stress_tests_lib_ignores_the_same_lines():
    """The stress tests run their own copy of the scan; the two lists must not drift."""
    lib = os.path.join(os.path.dirname(__file__), "../../tests/docker_scripts/stress_tests.lib")
    scan_line = next(
        line
        for line in open(lib, encoding="utf-8")
        if "Code: 499.*The specified key does not exist" in line
    )
    assert re.findall(r'-e "([^"]*)"', scan_line) == list(NO_SUCH_KEY_IGNORES)


@pytest.mark.parametrize("ignore", NO_SUCH_KEY_IGNORES)
def test_every_ignore_suppresses_its_own_line(tmp_path, ignore):
    line = (
        "2026.08.04 07:21:56.996964 [ 10623 ] {} <Debug> "
        f"{ignore}: Code: 499. DB::Exception: The specified key does not exist. (S3_ERROR)"
    )
    assert run_check(tmp_path, [line]) == 0
    # …and the same line without the ignored substring is still reported.
    assert run_check(tmp_path, [line.replace(ignore, "SomeOtherLogger")]) != 0
