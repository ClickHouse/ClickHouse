"""
Shell-level contract test for the atomic publish of `ci-checks.tsv` in
`ci/jobs/scripts/perf/compare.sh`.

`upload_results` is the last thing a perf-comparison shard does, and its failure
is swallowed by the `||:` on the call site (`time upload_results ||:`). It writes
the file through a ClickHouse `File(TSVWithNamesAndTypes)` table, so a failure
there - most often a full runner root filesystem - leaves an arbitrary byte
prefix behind. For a prefix that happens to end on a line boundary the parser in
`performance_tests.read_ci_checks_results` cannot tell it from a genuine short
run: two header lines plus the unconditional summary row is byte-for-byte what a
zero-query run writes. So for that whole class of prefixes, correctness rests
entirely on the file being published by rename, and the parser tests cannot cover
it because they drive Python only.

This test extracts the real publish block from `compare.sh` (between stable
anchor comments, so it cannot drift from the script it guards) and runs it under
bash with a stub `clickhouse-local`, in both directions: a failed write must
leave the final path ABSENT, and a successful one must leave it complete.

The `&&` chaining is what makes that hold: with errexit suppressed by the call
site's `||:`, a separate `mv` statement on the next line would still run after a
failed write and publish the torn file.
"""

import os
import subprocess

_COMPARE_SH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "jobs",
        "scripts",
        "perf",
        "compare.sh",
    )
)

_BEGIN = "# --- publish ci-checks.tsv atomically ---"
_END = "# --- end publish ci-checks.tsv ---"

# The block references these without defaults, so `set -u` kills it before the
# write without them. The values only have to be substitutable into the SQL - the
# stub never parses it.
_PREAMBLE = """set -exu
set -o pipefail
CHPC_REPORT_LOCAL_QUERY_SETTINGS=""
CHPC_REPORT_LOCAL_SERVER_SETTINGS=""
PR_TO_TEST=0
SHA_TO_TEST=6c5c34bf727ee7a2f0b0f8f4dbc1c0d9e1a2b3c4
CHPC_CHECK_START_TIMESTAMP=1700000000
CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME="Performance Comparison (amd_release, master_head, 4/6)"
CLICKHOUSE_PERFORMANCE_COMPARISON_CHECK_NAME_PREFIX=performance_comparison_amd_release_master_head_46
"""

# What a torn write leaves behind: a row cut mid-field. The test only checks
# which path it ends up at, not its contents beyond this marker.
_TORN = "torn-partial-row"

# The stub derives its target from the SQL it is given rather than hardcoding
# `ci-checks.tsv.tmp`, so it stays correct if the temporary name changes, and the
# same harness can drive a form of the block that writes the final path directly.
_STUB = """#!/bin/bash
query=""
while [ $# -gt 0 ]; do
    if [ "$1" = "--query" ]; then query="$2"; shift 2; continue; fi
    shift
done
target=$(printf '%s' "$query" | sed -n "s/.*File(TSVWithNamesAndTypes, '\\\\([^']*\\\\)').*/\\\\1/p")
printf 'STUB_TARGET=%s\\n' "$target" >&2
printf '%s' "$TORN_CONTENT" > "$target"
exit $STUB_EXIT_CODE
"""


def _extract_publish_block(text):
    begin = text.index(_BEGIN)
    end = text.index(_END, begin)
    # Keep the BEGIN anchor, drop the END marker line, and close the function the
    # anchors sit inside: everything after the END anchor (the commented-out
    # historical uploads and the closing brace) is not part of the contract.
    return text[begin:end].rstrip() + "\n}\n"


def _run_block(tmp_path, block, stub_exit_code):
    # `report.html` is real input: the block `sed`s the status and message out of
    # it into the SQL.
    (tmp_path / "report.html").write_text(
        "<!--status: failure-->\n<!--message: 5 slower-->\n", encoding="utf-8"
    )

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "clickhouse-local"
    stub.write_text(_STUB, encoding="utf-8")
    stub.chmod(0o755)

    # `REACHED_NEXT_STAGE` stands in for the statements that follow the call in
    # the real script: the `||:` on the call site means the job carries on in
    # both directions, and a change to that would be a change to the contract.
    script = (
        _PREAMBLE
        + "function upload_results\n{\n"
        + block
        + "\ntime upload_results ||:\necho REACHED_NEXT_STAGE\n"
    )

    env = dict(
        os.environ,
        PATH=f"{bin_dir}:{os.environ['PATH']}",
        STUB_EXIT_CODE=str(stub_exit_code),
        TORN_CONTENT=_TORN,
    )
    proc = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        check=False,
    )
    return proc


def _head_block():
    with open(_COMPARE_SH, encoding="utf-8") as f:
        return _extract_publish_block(f.read())


def test_a_successful_write_publishes_the_file(tmp_path):
    proc = _run_block(tmp_path, _head_block(), stub_exit_code=0)

    assert proc.returncode == 0, proc.stderr
    assert "REACHED_NEXT_STAGE" in proc.stdout
    # The rename ran, so the final path holds what the write produced and no
    # temporary file is left behind.
    assert (tmp_path / "ci-checks.tsv").read_text() == _TORN
    assert not (tmp_path / "ci-checks.tsv.tmp").exists()


def test_a_failed_write_leaves_no_file_at_the_final_path(tmp_path):
    proc = _run_block(tmp_path, _head_block(), stub_exit_code=1)

    # The failure is swallowed by the call site's `||:` and the job carries on -
    # unchanged behaviour, asserted so a change to the call site is visible here.
    assert proc.returncode == 0, proc.stderr
    assert "REACHED_NEXT_STAGE" in proc.stdout
    # The whole point: the torn bytes stay at the temporary path, and the final
    # path - the only one the importer reads - does not exist, so the shard
    # reports a missing file instead of a plausible-looking short one.
    assert not (tmp_path / "ci-checks.tsv").exists()
    assert (tmp_path / "ci-checks.tsv.tmp").read_text() == _TORN
