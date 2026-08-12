"""
Tests for `ci.jobs.performance_tests.build_check_results_children`.

The "Check Results" sub-result exists only to hang a per-query CIDB history link
on every slower/unstable query. Two properties of those rows are what a human
triaging a perf shard actually reads, and both are asserted here against real
report artifacts:

  * a row's status is the verdict compare.sh computed for that query, so a shard
    that passed does not render a list of red rows;
  * there is one row per query, not one per side - compare.sh emits both
    ("<query>::old" and "<query>::new") from `array join map('old', left, 'new',
    right)`, which otherwise doubles every count a reader sees.

The two fixtures are trimmed copies of real `result_performance_comparison_*.json`
artifacts, so the oracle is self-checking: the parent's own message states the
QUERY count, which must equal the number of rows.
"""

import base64
import collections
import inspect
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as performance_tests
from ci.jobs.performance_tests import (
    build_check_results_children,
    too_many_slow,
)
from ci.praktika.result import Result

FIXTURES = Path(__file__).parent / "fixtures"
CHECK_NAME_PATTERN = "%Performance%arm%master_head%"

# Statuses compare.sh emits for a query that did not come out clean
# (`multiIf(... 'slower', ... 'unstable', 'success')`).
NON_SUCCESS_STATUSES = ("slower", "unstable")


def _load(fixture):
    with open(FIXTURES / fixture, encoding="utf8") as f:
        return Result.from_dict(json.load(f))


def _parse_message_counts(message):
    """{'slower': 18, 'unstable': 1} from "6 too long, 9 faster, 18 slower, 1 unstable"."""
    return {
        status: int(count)
        for count, status in re.findall(
            r"(\d+)\s+(slower|unstable)", message
        )
    }


def _link_test_name(child):
    """The test_name the child's history link filters on."""
    labels = child.ext.get("labels") or []
    assert len(labels) == 1, f"expected one label on [{child.name}], got {labels}"
    label = labels[0]
    assert label["name"] == "query history", label
    assert label.get("link"), f"empty history link on [{child.name}]"
    query = base64.b64decode(label["link"].split("#", 1)[1]).decode("utf8")
    match = re.search(r"test_name = '([^']*)'", query)
    assert match, f"history link for [{child.name}] does not filter test_name"
    return match.group(1)


def _base_name(name):
    return name.removesuffix("::old").removesuffix("::new")


# ---------------------------------------------------------------------------
# The two real artifacts. Each case is (fixture, parent status, pre-fix row count).
# ---------------------------------------------------------------------------

CASES = [
    # The shard reported in PR #111992: it PASSED, yet rendered 22 red rows for
    # 11 unstable queries.
    ("perf_check_results_green_shard.json", Result.Status.OK, 22),
    # A genuinely failing release_base shard - the negative control, and the
    # only fixture carrying `slower` rows.
    ("perf_check_results_red_shard.json", Result.Status.FAIL, 38),
]


def test_row_count_equals_query_count_from_parent_message():
    # The parent's message counts QUERIES, so it is an independent oracle for
    # the number of rows: it comes from report.py, not from this code path.
    for fixture, _, pre_fix_count in CASES:
        root = _load(fixture)
        expected = sum(_parse_message_counts(root.info).values())
        # A fixture whose message lost its counts would make every assertion
        # below trivially true.
        assert expected > 0, f"{fixture}: no counts in {root.info!r}"
        children = build_check_results_children(
            root.get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
        )
        assert len(children) == expected, (
            f"{fixture}: {len(children)} rows for a parent reporting "
            f"{expected} queries ({root.info!r})"
        )
        # Both sides of every query used to be listed.
        assert pre_fix_count == 2 * expected, fixture
        assert len(children) == pre_fix_count // 2, fixture


def test_row_count_equals_distinct_query_names():
    for fixture, _, _ in CASES:
        root = _load(fixture)
        tests = root.get_sub_result_by_name("Tests")
        distinct = {
            _base_name(r.name)
            for r in tests.results
            if r.status in NON_SUCCESS_STATUSES
        }
        assert distinct, fixture
        children = build_check_results_children(tests, CHECK_NAME_PATTERN)
        assert {c.name for c in children} == distinct, fixture


def test_rows_carry_the_compare_sh_verdict_not_fail():
    # A hardcoded FAIL is what made a passing shard render red.
    for fixture, _, _ in CASES:
        root = _load(fixture)
        tests = root.get_sub_result_by_name("Tests")
        children = build_check_results_children(tests, CHECK_NAME_PATTERN)
        assert children, fixture
        verdicts = {
            _base_name(r.name): r.status
            for r in tests.results
            if r.status in NON_SUCCESS_STATUSES
        }
        for child in children:
            assert child.status != Result.Status.FAIL, f"{fixture}: {child.name}"
            assert child.status in NON_SUCCESS_STATUSES, (
                f"{fixture}: {child.name} has status {child.status!r}"
            )
            assert child.status == verdicts[child.name], f"{fixture}: {child.name}"


def test_expected_verdict_mix_per_fixture():
    # Pinned so a fixture swap cannot quietly drop `slower` coverage: only the
    # red shard exercises it, and it is the status the renderer paints red.
    green = build_check_results_children(
        _load(CASES[0][0]).get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
    )
    red = build_check_results_children(
        _load(CASES[1][0]).get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
    )
    assert collections.Counter(c.status for c in green) == {"unstable": 11}
    assert collections.Counter(c.status for c in red) == {
        "slower": 18,
        "unstable": 1,
    }


def test_displayed_names_carry_no_side_suffix():
    for fixture, _, _ in CASES:
        root = _load(fixture)
        children = build_check_results_children(
            root.get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
        )
        assert children, fixture
        for child in children:
            assert "::" not in child.name, f"{fixture}: {child.name}"


def test_history_link_keeps_the_suffixed_name():
    # `build_perf_query_history_link` filters `test_name = '...'`, an exact
    # match, and CIDB stores the suffixed name. Handing it the displayed name
    # returns zero rows for every query, with no visible error.
    for fixture, _, _ in CASES:
        root = _load(fixture)
        children = build_check_results_children(
            root.get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
        )
        assert children, fixture
        for child in children:
            assert _link_test_name(child) == f"{child.name}::new", fixture


def test_only_the_candidate_side_is_kept():
    tests = Result(
        name="Tests",
        status=Result.Status.OK,
        results=[
            Result(name="q #1::old", status="unstable", duration=1.0),
            Result(name="q #1::new", status="unstable", duration=2.0),
        ],
    )
    children = build_check_results_children(tests, CHECK_NAME_PATTERN)
    assert [c.name for c in children] == ["q #1"]
    # The candidate side's timing, not the reference side's.
    assert children[0].duration == 2.0


def test_successful_queries_are_not_listed():
    tests = Result(
        name="Tests",
        status=Result.Status.OK,
        results=[
            Result(name="q #1::old", status="success"),
            Result(name="q #1::new", status="success"),
            Result(name="q #2::old", status="slower"),
            Result(name="q #2::new", status="slower"),
        ],
    )
    children = build_check_results_children(tests, CHECK_NAME_PATTERN)
    assert [(c.name, c.status) for c in children] == [("q #2", "slower")]


def test_suffixless_row_passes_through_unchanged():
    # Nothing emits such a row today. Dropping it silently would be the wrong
    # failure mode if compare.sh ever stopped splitting sides, so the filter
    # skips "::old" instead of requiring "::new".
    tests = Result(
        name="Tests",
        status=Result.Status.OK,
        results=[Result(name="q #7", status="slower", duration=3.0)],
    )
    children = build_check_results_children(tests, CHECK_NAME_PATTERN)
    assert [c.name for c in children] == ["q #7"]
    assert _link_test_name(children[0]) == "q #7"


def test_suffix_stripping_is_anchored_to_the_end():
    tests = Result(
        name="Tests",
        status=Result.Status.OK,
        results=[Result(name="q::new #1::new", status="unstable")],
    )
    children = build_check_results_children(tests, CHECK_NAME_PATTERN)
    assert [c.name for c in children] == ["q::new #1"]


def test_both_sides_of_a_query_agree_on_status():
    # Selecting one side is only sound because `test_status` is computed from
    # the per-query columns of report/queries.tsv before compare.sh array-joins
    # `map('old', left, 'new', right)`. If that ever moves after the join, the
    # sides can disagree and this assertion says why the selection is wrong.
    for fixture, _, _ in CASES:
        rows = _load(fixture).get_sub_result_by_name("Tests").results
        by_query = collections.defaultdict(set)
        sides = collections.Counter()
        for r in rows:
            by_query[_base_name(r.name)].add(r.status)
            sides[_base_name(r.name)] += 1
        disagreeing = {q: s for q, s in by_query.items() if len(s) > 1}
        assert not disagreeing, f"{fixture}: {disagreeing}"
        # Every query really does carry both sides, or the check above is
        # vacuous: a one-sided query trivially agrees with itself.
        assert set(sides.values()) == {2}, (
            f"{fixture}: side counts {collections.Counter(sides.values())}"
        )


def test_per_query_rows_cannot_change_the_job_status():
    # "Check Results" is built with an explicit status, and `Result.create_from`
    # aggregates children only when no status is given
    # (`if results and not status`). That is why correcting these rows cannot
    # turn a passing shard red. If the guard goes away, this fails here instead
    # of reddening every perf shard in CI.
    failing = [Result(name="q #1", status=Result.Status.FAIL, info="unstable")]
    assert (
        Result.create_from(
            name="Check Results", results=failing, status=Result.Status.OK
        ).status
        == Result.Status.OK
    )
    assert (
        Result.create_from(name="Check Results", results=failing).status
        == Result.Status.FAIL
    )


def test_parent_status_is_preserved_for_both_fixtures():
    # The negative control: a genuinely failing shard must stay FAIL, and a
    # passing one must stay OK, with the corrected rows underneath.
    for fixture, expected_status, _ in CASES:
        root = _load(fixture)
        children = build_check_results_children(
            root.get_sub_result_by_name("Tests"), CHECK_NAME_PATTERN
        )
        parent = Result(
            name="Check Results",
            status=root.status,
            info=root.info,
            results=children,
        )
        job = Result.create_from(
            name=root.name,
            results=[root.get_sub_result_by_name("Tests"), parent],
            info=root.info,
        )
        assert parent.status == expected_status, fixture
        assert job.status == expected_status, fixture


def test_slow_query_gate_is_untouched():
    # The job verdict comes from the slower-query gate, not from these rows.
    assert too_many_slow("11 slower") is True
    assert too_many_slow("10 slower") is False


def test_renderer_paints_the_two_verdicts():
    # There is no JS runner in ci/tests, so assert the mapping at its source.
    # `shouldShowRow` counts 'slower' as a failure and 'unstable' as a warning;
    # `getStatusClass` has to agree, or a slower query renders gray.
    source = (
        Path(__file__).parents[1] / "praktika" / "json.html"
    ).read_text(encoding="utf8")
    assert "if (lowerStatus === 'slower') return 'status-fail';" in source
    assert "status_ === 'slower')" in source
    assert "status_ === 'unstable'" in source
    # 'unstable' must NOT be painted as a failure: it is a warning.
    assert "=== 'unstable') return 'status-fail'" not in source


def test_hardcoded_fail_is_not_reintroduced():
    source = inspect.getsource(performance_tests.build_check_results_children)
    assert "Result.Status.FAIL" not in source, (
        "A hardcoded FAIL renders a passing perf shard as a list of red rows. "
        "Pass compare.sh's verdict (tr.status) through instead."
    )


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("All perf Check Results children tests passed.")
