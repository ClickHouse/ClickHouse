"""
Tests for the "No such key" server-log health checks built by
`ClickHouseProc.check_fatal_messages_in_logs` ("Lost s3 keys" and
"S3_ERROR No such key thrown ...").

Regression coverage for a chronic `Stateless tests (*, s3 storage, *)` failure
(e.g. master efccb01cfb8d5c3cd65e763059f13337283b473e, and PRs 110121/111029
before it) where all 4050 tests passed but both checks failed on a benign
filesystem-cache background-download error: the cache background download
worker (`CacheMetadata`) keeps prefetching a partially downloaded file segment
whose remote object was meanwhile removed (DROP/mutation/part removal), so the
GET returns `Code: 499` `NoSuchKey`, the download is marked as failed and the
data is simply re-read from the source later - nothing is lost.

`tests/docker_scripts/stress_tests.lib` already ignores this line (PR #108489),
the functional-tests checks did not.

A `No such key` reported from any other code path (e.g. a foreground merge read)
must still fail the check.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.result import Result

NO_SUCH_KEY_CHECKS = [
    "Lost s3 keys",
    "S3_ERROR No such key thrown (in clickhouse-server.log or clickhouse-server.err.log)",
]

# Verbatim from the master run linked above.
BENIGN_BACKGROUND_DOWNLOAD = (
    "2026.07.27 11:27:16.602006 [ 781 ] {} <Error> CacheMetadata: Error during background download of "
    "1fd00bda8ebe086136a7332edb776c3f:0 (File segment: [0, 2038035], key: 1fd00bda8ebe086136a7332edb776c3f, "
    "state: PARTIALLY_DOWNLOADED_NO_CONTINUATION, downloaded size: 1048576, reserved size: 1048576, "
    "downloader id: None:DEFAULT_THREAD_POOL:781, current write offset: 1048576, "
    "caller id: None:DEFAULT_THREAD_POOL:781, kind: Regular, unbound: 0, background download: 1): "
    "Code: 499. DB::Exception: The specified key does not exist. This error happened for S3 disk: "
    "while reading key: iij-first-random-part/new-style-prefix/qmf/ednctraocbznyybcdgqccrfbxmrzz, "
    "from bucket: test. (S3_ERROR), Stack trace (when copying this message, always include the lines below):"
)

GENUINE_LOST_KEY = (
    "2026.07.27 11:30:00.000000 [ 782 ] {q} <Error> virtual bool DB::MergePlainMergeTreeTask::executeStep(): "
    "Exception is in merge_task.: Code: 499. DB::Exception: The specified key does not exist. "
    "This error happened for S3 disk: while reading key: abc/def, from bucket: test. (S3_ERROR)"
)


def _run_log_checks(tmp_path, lines):
    log_dir = tmp_path / "clickhouse-server"
    log_dir.mkdir(parents=True, exist_ok=True)
    for log in ("clickhouse-server.log", "clickhouse-server.err.log"):
        (log_dir / log).write_text("\n".join(lines) + "\n")

    proc = ClickHouseProc()
    proc.log_dir = str(log_dir)
    # dmesg is not available in the unit-test job and is irrelevant here
    proc.check_ch_is_oom_killed = lambda: None
    return {result.name: result for result in proc.check_fatal_messages_in_logs()}


def test_benign_background_download_no_such_key_is_ignored(tmp_path):
    results = _run_log_checks(tmp_path, [BENIGN_BACKGROUND_DOWNLOAD])
    for name in NO_SUCH_KEY_CHECKS:
        assert results[name].status == Result.Status.OK, results[name].info


def test_no_such_key_from_other_code_path_still_fails(tmp_path):
    results = _run_log_checks(tmp_path, [BENIGN_BACKGROUND_DOWNLOAD, GENUINE_LOST_KEY])
    for name in NO_SUCH_KEY_CHECKS:
        assert results[name].status == Result.Status.FAIL, results[name].info
        assert "MergePlainMergeTreeTask" in results[name].info
        assert "Error during background download" not in results[name].info
