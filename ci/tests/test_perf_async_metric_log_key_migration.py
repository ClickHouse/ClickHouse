"""
Shell-level contract test for the asynchronous metric log projection in
`ci/jobs/scripts/perf/compare.sh`.

`system.asynchronous_metric_log` gained a `key` column when the per-entity
asynchronous metrics (per CPU core, block device, network interface, disk, ...)
were collapsed into key-value metrics. The perf report joins the two servers'
logs on the metric name alone, so a side that carries a `key` column has to be
projected back to the flat per-entity names (`BlockReadBytes_sda`) that the
report, and `perf_metric_changes_v1` behind it, have always used.

The side to project cannot be hardcoded. While the change is only in a pull
request the left (master) side has no `key` column and the right side has one;
once it merges, both sides have one. Projecting a fixed side is silently wrong
for the other case: `l.metric = 'BlockReadBytes'` never equals
`r.metric = 'BlockReadBytes_sda'`, so the `ASOF JOIN` drops every converted
family instead of failing, and the metrics disappear from the report unnoticed.

`async_metric_log_select` is a pure function of a dump's header line, so this
test extracts it from the script (by the same anchors bash sees, so it cannot
drift from what runs) and drives it under the script's own
`set -exu -o pipefail` - which is what forces the header test to be
pipeline-free, a pipeline there reports `SIGPIPE` as a failed header read.
"""

import os
import re
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

# The script's own options. The function has to work under them, not under a
# permissive shell: `set -u` rejects an unset array expansion, and `pipefail`
# turns the `SIGPIPE` of an early-exiting reader into a failed header read.
_PREAMBLE = "set -exu\nset -o pipefail\n"

_HEADER_WITH_KEY = "hostname\tevent_date\tevent_time\tmetric\tkey\tvalue\n"
_HEADER_WITHOUT_KEY = "hostname\tevent_date\tevent_time\tmetric\tvalue\n"

_PLAIN_SELECT = "metric, event_time, value"


def _extract_function(text, name):
    begin = text.index(f"function {name}\n")
    # The functions in this script are written with the closing brace in column
    # zero, which is what makes this unambiguous.
    end = text.index("\n}\n", begin) + len("\n}\n")
    return text[begin:end]


def _compare_sh():
    with open(_COMPARE_SH, encoding="utf-8") as f:
        return f.read()


def _select_for(tmp_path, header, dump_name="async-metric-log.tsv"):
    """The projection the script picks for a dump with the given header line."""
    dump = tmp_path / dump_name
    dump.write_text(header, encoding="utf-8")

    script = (
        _PREAMBLE
        + _extract_function(_compare_sh(), "async_metric_log_select")
        + f"\nasync_metric_log_select {dump_name}\n"
    )
    proc = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        cwd=str(tmp_path),
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_a_dump_with_a_key_column_is_projected_to_the_flat_names(tmp_path):
    select = _select_for(tmp_path, _HEADER_WITH_KEY)

    # The scalar rows keep their name, and every keyed family is folded back into
    # the name the report knew before the change.
    assert "key = '', metric," in select
    assert "concat(metric, '_', key)) AS metric," in select
    assert "concat('AsyncLogging', key, 'QueueSize')" in select


def test_a_dump_without_a_key_column_is_taken_as_is(tmp_path):
    # A pre-change dump is already in the flat form, and naming `key` in the
    # projection would fail against a file that has no such column.
    assert _select_for(tmp_path, _HEADER_WITHOUT_KEY).strip() == _PLAIN_SELECT


def test_a_column_whose_name_merely_starts_with_key_is_not_a_key_column(tmp_path):
    assert _select_for(tmp_path, "metric\tkeyspace\tvalue\n").strip() == _PLAIN_SELECT


def test_an_empty_dump_does_not_fail_the_report(tmp_path):
    # Both dumps are produced with `||:`, so a server that died leaves an empty
    # file behind. Reading its (absent) header must not take the whole report
    # down through `set -e` before the query even runs.
    assert _select_for(tmp_path, "").strip() == _PLAIN_SELECT


def test_both_sides_of_the_join_go_through_the_projection():
    text = _compare_sh()

    # Each side is projected according to its own header ...
    for side in ("left", "right"):
        assert (
            f"create view {side}_async_metric_log as" in text
        ), f"the {side} side is not projected through a view"
        assert (
            f"$(async_metric_log_select {side}-async-metric-log.tsv)" in text
        ), f"the {side} side does not choose its projection from its own header"

    # ... and the join reads the projected views, not the raw dumps: reading one
    # side raw is exactly the asymmetry that makes the converted families vanish.
    join = re.search(r"create table metrics engine File.*?;", text, re.DOTALL)
    assert join is not None, "the metrics table is no longer created here"
    assert "left_async_metric_log l" in join.group(0)
    assert "right_async_metric_log r" in join.group(0)
    assert "file(" not in join.group(0)
