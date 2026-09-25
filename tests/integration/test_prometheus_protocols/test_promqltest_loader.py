import math
import textwrap
from pathlib import Path

from . import promqltest_loader as loader
from . import update_compliance_baseline


def test_parse_duration():
    assert loader.parse_duration("0") == 0.0
    assert loader.parse_duration("50m") == 3000.0
    assert loader.parse_duration("1m30s") == 90.0
    assert loader.parse_duration("15s") == 15.0
    assert loader.parse_duration("1ms") == 0.001


def test_expand_arithmetic_samples():
    samples = loader.expand_sample_token("0+10x10")
    assert len(samples) == 11
    assert samples[0].value == 0
    assert samples[-1].value == 100


def test_expand_missing_and_stale():
    assert loader.expand_sample_token("_")[0].missing is True
    assert loader.expand_sample_token("stale")[0].stale is True


def test_stale_marker_scenario_exclusion(tmp_path: Path):
    path = tmp_path / "stale.test"
    path.write_text(
        textwrap.dedent(
            """
            load 10s
              metric 0 stale 1

            eval instant at 20s metric
              metric 1

            clear
            load 10s
              metric 1

            eval instant at 0s metric
              metric stale
            """
        )
    )
    cases = [scenario.evals[0] for scenario in loader.parse_test_file(path)]
    assert len(cases) == 2
    for case in cases:
        assert case.exclusion_reason() == "stale_marker"
        assert loader.classify_eval(case) == "excluded_assertion"


def test_native_histogram_token():
    samples = loader.expand_sample_token("{{schema:0 sum:1 count:1}}x9")
    assert len(samples) == 10
    assert all(s.native_histogram for s in samples)


def test_parse_sort_ordered_and_trig_and_fail(tmp_path: Path):
    text = textwrap.dedent(
        """
        load 5m
          http_requests{group="production", instance="0"} 0+10x10
          http_requests{group="canary", instance="1"} 0+40x10

        eval instant at 50m sort(http_requests)
            expect ordered
            http_requests{group="production", instance="0"} 100
            http_requests{group="canary", instance="1"} 400

        eval instant at 50m sort_desc(http_requests)
            expect ordered
            http_requests{group="canary", instance="1"} 400
            http_requests{group="production", instance="0"} 100

        eval instant at 0 acos(vector(1))
            {} 0

        eval instant at 50m present_over_time(http_requests[5m])
            {group="production", instance="0"} 1
            {group="canary", instance="1"} 1

        eval instant at 50m http_requests
            expect info
            http_requests{group="production", instance="0"} 100

        eval instant at 0 label_replace(http_requests, "~invalid", "", "src", "(.*)")
            expect fail

        eval instant at 0 dup_metric + on() dup_metric
            expect fail msg: vector cannot contain metrics with the same labelset

        eval instant at 50m resets(http_requests_histogram[6m])
            {path="/foo"} 0

        load 5m
          http_requests_histogram{path="/foo"} {{schema:0 sum:1 count:1}}x9

        eval instant at 50m resets(http_requests_histogram[6m])
            {path="/foo"} {{schema:0 sum:1 count:1}}
        """
    )
    path = tmp_path / "fixture.test"
    path.write_text(text)
    scenarios = loader.parse_test_file(path)
    evals = [ev for sc in scenarios for ev in sc.evals]
    by_expr = {ev.expr: ev for ev in evals}
    assert by_expr["sort(http_requests)"].expect_ordered is True
    assert by_expr["sort_desc(http_requests)"].expect_ordered is True
    assert by_expr["acos(vector(1))"].expected_series[0].samples[0].value == 0
    assert any("present_over_time" in expr for expr in by_expr)
    assert by_expr["http_requests"].exclusion_reason() == "annotation_assertion"
    assert loader.classify_eval(by_expr["http_requests"]) == "excluded_assertion"
    bare_fail = by_expr['label_replace(http_requests, "~invalid", "", "src", "(.*)")']
    assert bare_fail.expect_fail
    assert bare_fail.exclusion_reason() is None
    msg_fail = by_expr["dup_metric + on() dup_metric"]
    assert msg_fail.expect_fail
    assert msg_fail.exclusion_reason() == "expect_fail_diagnostic"
    assert loader.classify_eval(msg_fail) == "excluded_assertion"
    hist = [ev for ev in evals if ev.expr.startswith("resets(http_requests_histogram")]
    assert hist[-1].exclusion_reason() is not None
    assert loader.classify_eval(hist[-1]) == "excluded_native_histogram"


def test_native_histogram_selector_exclusion(tmp_path: Path):
    text = textwrap.dedent(
        """
        load 5m
          http_requests{path="/foo"} 0+10x10
          http_requests{path="/bar"} {{schema:0 sum:1 count:1}}x10

        eval instant at 50m rate(http_requests[5m])
            {path="/bar"} 1

        eval instant at 50m sum({path="/bar"})
            {} 1

        eval instant at 50m {__name__=~"http_requests"}
            {path="/bar"} 1

        eval instant at 50m http_requests{path!="/bar"}
            http_requests{path="/foo"} 100

        eval instant at 50m label_replace(http_requests{path="/foo"}, "dst", "http_requests", "path", "(.*)")
            http_requests{path="/foo", dst="http_requests"} 100
        """
    )
    path = tmp_path / "histogram.test"
    path.write_text(text)
    scenarios = loader.parse_test_file(path)
    by_expr = {ev.expr: ev for sc in scenarios for ev in sc.evals}
    for expr in ("rate(http_requests[5m])", 'sum({path="/bar"})', '{__name__=~"http_requests"}'):
        assert loader.classify_eval(by_expr[expr]) == "excluded_native_histogram", expr
    for expr in (
        'http_requests{path!="/bar"}',
        'label_replace(http_requests{path="/foo"}, "dst", "http_requests", "path", "(.*)")',
    ):
        assert by_expr[expr].exclusion_reason() is None, expr


def test_snapshot_manifest_is_complete():
    scenarios = loader.parse_all_files()
    loader.assert_manifest_complete(scenarios)
    ids = loader.manifest_eval_ids(scenarios)
    assert len(ids) == 1129


def test_clear_isolates_scenarios(tmp_path: Path):
    text = textwrap.dedent(
        """
        load 15s
          bar 0 1 10
        eval range from 0 to 1m step 30s sum_over_time(bar[30s])
          {} 0 11
        clear
        load 15s
          baz 5
        eval instant at 0 baz
          baz 5
        """
    )
    path = tmp_path / "iso.test"
    path.write_text(text)
    scenarios = loader.parse_test_file(path)
    assert len(scenarios) == 2
    assert scenarios[0].evals[0].kind == "range"
    assert scenarios[1].evals[0].expr == "baz"


def test_commands_follow_source_order(tmp_path: Path):
    text = textwrap.dedent(
        """
        load 10s
          m 1
        eval instant at 0 m
          m 1
        load 10s
          n 2
        eval instant at 0 n
          n 2
        """
    )
    path = tmp_path / "order.test"
    path.write_text(text)
    (scenario,) = loader.parse_test_file(path)
    assert [type(command) for command in scenario.commands] == [
        loader.LoadBlock,
        loader.EvalCase,
        loader.LoadBlock,
        loader.EvalCase,
    ]
    assert [block.series[0].metric for block in scenario.loads] == ["m", "n"]
    assert [case.expr for case in scenario.evals] == ["m", "n"]


def test_compare_ordered_pass_and_fail():
    case = loader.EvalCase(
        eval_id="t:1",
        file_name="t.test",
        line=1,
        kind="instant",
        expr="sort(m)",
        time_s=50,
        expect_ordered=True,
        expected_series=[
            loader.parse_series_line('m{g="a"} 1'),
            loader.parse_series_line('m{g="b"} 2'),
        ],
    )
    tsv_ok = (
        "[('__name__','m'),('g','a')]\t1970-01-01 00:00:50.000\t1\n"
        "[('__name__','m'),('g','b')]\t1970-01-01 00:00:50.000\t2\n"
    )
    status, _ = loader.compare_eval(case, tsv_ok, None)
    assert status == "passed"
    tsv_bad = (
        "[('__name__','m'),('g','b')]\t1970-01-01 00:00:50.000\t2\n"
        "[('__name__','m'),('g','a')]\t1970-01-01 00:00:50.000\t1\n"
    )
    status, _ = loader.compare_eval(case, tsv_bad, None)
    assert status == "failed"


def test_compare_range_timestamps():
    def make_case(values: str) -> loader.EvalCase:
        return loader.EvalCase(
            eval_id="t:4",
            file_name="t.test",
            line=4,
            kind="range",
            expr="m",
            time_s=60,
            start_s=0,
            end_s=60,
            step_s=30,
            expected_series=[loader.parse_series_line(f'm{{g="a"}} {values}')],
        )

    tsv = (
        "[('__name__','m'),('g','a')]\t"
        "[('1970-01-01 00:00:00.000',1),('1970-01-01 00:00:30.000',2),"
        "('1970-01-01 00:01:00.000',3)]\n"
    )
    status, _ = loader.compare_eval(make_case("1 2 3"), tsv, None)
    assert status == "passed"
    extra = tsv.replace(
        ")]\n", "),('1970-01-01 00:01:30.000',4)]\n"
    )
    status, _ = loader.compare_eval(make_case("1 2 3"), extra, None)
    assert status == "failed"
    duplicate = tsv.replace(
        ")]\n", "),('1970-01-01 00:01:00.000',3)]\n"
    )
    status, _ = loader.compare_eval(make_case("1 2 3"), duplicate, None)
    assert status == "failed"
    shuffled = (
        "[('__name__','m'),('g','a')]\t"
        "[('1970-01-01 00:00:30.000',2),('1970-01-01 00:00:00.000',1),"
        "('1970-01-01 00:01:00.000',3)]\n"
    )
    status, _ = loader.compare_eval(make_case("1 2 3"), shuffled, None)
    assert status == "failed"
    status, _ = loader.compare_eval(make_case("1 _ 3"), tsv, None)
    assert status == "failed"


def test_parse_sql_result_scalar_two_columns():
    rows = loader.parse_sql_result(
        "1970-01-01 00:00:00.000\t1\n"
        "1970-01-01 00:00:01.000\t1.234e-05\n"
        "1970-01-01 00:00:02.000\tNaN\n"
        "1970-01-01 00:00:03.000\t+Inf\n"
        "1970-01-01 00:00:04.000\t-Inf\n"
    )
    assert [row["metric"] for row in rows] == [{}, {}, {}, {}, {}]
    assert [row["timestamp"] for row in rows] == [
        "1970-01-01 00:00:00.000",
        "1970-01-01 00:00:01.000",
        "1970-01-01 00:00:02.000",
        "1970-01-01 00:00:03.000",
        "1970-01-01 00:00:04.000",
    ]
    assert rows[0]["value"] == 1
    assert rows[1]["value"] == 1.234e-05
    assert math.isnan(rows[2]["value"])
    assert rows[3]["value"] == math.inf
    assert rows[4]["value"] == -math.inf


def test_compare_scalar_requires_unlabeled_row():
    case = loader.EvalCase(
        eval_id="t:5",
        file_name="t.test",
        line=5,
        kind="instant",
        expr="scalar(m)",
        time_s=0,
        expected_scalar=1.0,
        has_scalar=True,
    )
    status, _ = loader.compare_eval(case, "1970-01-01 00:00:00.000\t1\n", None)
    assert status == "passed"
    status, _ = loader.compare_eval(case, "[]\t1970-01-01 00:00:00.000\t1\n", None)
    assert status == "passed"
    status, reason = loader.compare_eval(
        case, "[('__name__','m')]\t1970-01-01 00:00:00.000\t1\n", None
    )
    assert status == "failed"
    assert "labels" in reason
    status, _ = loader.compare_eval(case, "[('__name__','m')]\t1\n", None)
    assert status == "failed"
    status, _ = loader.compare_eval(case, "1970-01-01 00:00:00.000\tnot-a-number\n", None)
    assert status == "failed"
    status, _ = loader.compare_eval(case, "garbage\t1\n", None)
    assert status == "failed"


def test_compare_vector_rejects_scalar_shaped_sql():
    cases = {
        case.eval_id: case
        for scenario in loader.parse_test_file(loader.TESTDATA_DIR / "functions.test")
        for case in scenario.evals
    }
    for eval_id in ("functions.test:597", "functions.test:2147"):
        assert cases[eval_id].has_scalar is False
        status, _ = loader.compare_eval(
            cases[eval_id], "1970-01-01 00:00:00.000\t1\n", None
        )
        assert status == "failed"


def test_compare_expect_fail(tmp_path: Path):
    path = tmp_path / "expect_fail.test"
    path.write_text(
        textwrap.dedent(
            """
            eval instant at 0 bad()
                expect fail

            eval_fail instant at 0 bad()
            """
        )
    )
    cases = loader.parse_test_file(path)[0].evals
    assert len(cases) == 2
    assert all(case.expect_fail for case in cases)
    errors = (
        "Code: 48. DB::Exception: Function foo is unavailable",
        "DB::Exception: Feature unavailable (NOT_IMPLEMENTED)",
    )
    for case in cases:
        for error in errors:
            status, _ = loader.compare_eval(case, "", error)
            assert status == "passed"
        status, _ = loader.compare_eval(case, "[('__name__','x')]\t0\t1\n", None)
        assert status == "failed"


def test_unsupported_not_implemented():
    case = loader.EvalCase(
        eval_id="t:3",
        file_name="t.test",
        line=3,
        kind="instant",
        expr="foo()",
        time_s=0,
    )
    errors = (
        "Function foo is not implemented",
        "Code: 48. DB::Exception: Function quantile_over_time is unavailable",
        "DB::Exception: Feature unavailable (NOT_IMPLEMENTED)",
        "Function quantile_over_time is not supported",
    )
    for error in errors:
        status, _ = loader.compare_eval(case, "", error)
        assert status == "unsupported", error


def test_insert_sql_skips_native_histogram():
    spec = loader.parse_series_line(
        'http_requests_histogram{path="/foo"} {{schema:0 sum:1 count:1}}x2'
    )
    assert spec.native_histogram
    assert loader.series_insert_sql("t", 300, spec) is None
    spec2 = loader.parse_series_line('http_requests{path="/foo"} 1 2 3')
    sql = loader.series_insert_sql("t", 300, spec2)
    assert sql is not None
    assert "toDateTime64(0, 9)" in sql
    assert "toDateTime64(600, 9)" in sql
    assert loader.series_insert_values(300, spec2) in sql


def test_compliance_record_schema_version_2():
    compliance = {"passed": 1, "total": 1}
    payload = {
        "schema_version": 2,
        "suites": {
            "compliance": compliance,
            "extended_support": {"passed": 2, "total": 2},
        },
    }
    assert update_compliance_baseline._compliance_record(payload) == compliance
