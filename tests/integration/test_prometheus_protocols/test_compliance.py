"""
PromQL compliance test based on https://github.com/prometheus/compliance/blob/main/promql/promql-test-queries.yml

Generates synthetic demo-service data in OpenMetrics format, ingests it into
both a reference Prometheus server and ClickHouse, then runs every query from
the upstream compliance suite and reports a score.
"""

import json
import math
import os
import re
import requests
import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)
from .generate_compliance_data import (
    generate as generate_openmetrics,
    BASE_TIME,
    DATA_START,
    DATA_END,
)


# ── Cluster setup ────────────────────────────────────────────────────────────

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
    with_prometheus_receiver=True,
)


# ── Query time parameters ────────────────────────────────────────────────────

QUERY_START = BASE_TIME - 120
QUERY_END = BASE_TIME
QUERY_STEP = 60

FLOAT_FRACTION = 0.00001
FLOAT_MARGIN = 0.0001


# ── OpenMetrics parser ───────────────────────────────────────────────────────

_METRIC_LINE_RE = re.compile(
    r'^([a-zA-Z_:][a-zA-Z0-9_:]*)'  # metric name
    r'(?:\{([^}]*)\})?'              # optional {labels}
    r'\s+(\S+)'                      # value
    r'(?:\s+(\S+))?'                 # optional timestamp
    r'\s*$'
)

_LABEL_RE = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def parse_openmetrics_file(path):
    """
    Parse an OpenMetrics text file into a list of
    (labels_dict, {timestamp_float: value_float}) tuples suitable for
    convert_time_series_to_protobuf.
    """
    series_map = {}  # (name, frozenset(label_items)) -> (labels_dict, samples_dict)

    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue

            m = _METRIC_LINE_RE.match(line)
            if not m:
                continue

            name = m.group(1)
            labels_str = m.group(2) or ""
            value_str = m.group(3)
            ts_str = m.group(4)

            raw_labels = dict(_LABEL_RE.findall(labels_str))
            raw_labels["__name__"] = name
            # Remote Write requires labels sorted lexicographically by name.
            labels = dict(sorted(raw_labels.items()))

            value = _parse_float(value_str)
            timestamp = float(ts_str) if ts_str else 0.0

            key = (name, frozenset(labels.items()))
            if key not in series_map:
                series_map[key] = (labels, {})
            series_map[key][1][timestamp] = value

    return list(series_map.values())


def _parse_float(s):
    s_lower = s.lower()
    if s_lower in ("+inf", "inf"):
        return float("inf")
    if s_lower == "-inf":
        return float("-inf")
    if s_lower == "nan":
        return float("nan")
    return float(s)


# ── Variant expansion (mirrors promql/testcases/expand.go) ──────────────────

VARIANT_VALUES = {
    "range": ["1s", "15s", "1m", "5m", "15m", "1h"],
    "offset": ["1m", "5m", "10m"],
    "simpleAggrOp": ["sum", "avg", "max", "min", "count", "stddev", "stdvar"],
    "simpleTimeAggrOp": [
        "sum", "avg", "max", "min", "count", "stddev", "stdvar", "absent", "last",
    ],
    "topBottomOp": ["topk", "bottomk"],
    "quantile": ["-0.5", "0.1", "0.5", "0.75", "0.95", "0.90", "0.99", "1", "1.5"],
    "arithBinOp": ["+", "-", "*", "/", "%", "^"],
    "compBinOp": ["==", "!=", "<", ">", "<=", ">="],
    "binOp": ["+", "-", "*", "/", "%", "^", "==", "!=", "<", ">", "<=", ">="],
    "simpleMathFunc": [
        "abs", "ceil", "floor", "exp", "sqrt", "ln", "log2", "log10", "round",
    ],
    "extrapolatedRateFunc": ["delta", "rate", "increase"],
    "clampFunc": ["clamp_min", "clamp_max"],
    "instantRateFunc": ["idelta", "irate"],
    "dateFunc": [
        "day_of_month", "day_of_week", "days_in_month",
        "hour", "minute", "month", "year",
    ],
    "smoothingFactor": ["0.1", "0.5", "0.8"],
    "trendFactor": ["0.1", "0.5", "0.8"],
}


def _expand_query(template, variant_args):
    if not variant_args:
        return [template]
    first_arg = variant_args[0]
    rest = variant_args[1:]
    results = []
    for value in VARIANT_VALUES[first_arg]:
        expanded = template.replace("{{." + first_arg + "}}", value)
        results.extend(_expand_query(expanded, rest))
    return results


# ── Test case definitions from prometheus/compliance promql-test-queries.yml ─
# (query_template, variant_args, should_fail)

COMPLIANCE_TEST_CASES = [
    # Scalar literals
    ("42", [], False),
    ("1.234", [], False),
    (".123", [], False),
    ("1.23e-3", [], False),
    ("0x3d", [], False),
    ("Inf", [], False),
    ("+Inf", [], False),
    ("-Inf", [], False),
    ("NaN", [], False),

    # Vector selectors
    ("demo_memory_usage_bytes", [], False),
    ('{__name__="demo_memory_usage_bytes"}', [], False),
    ('demo_memory_usage_bytes{type="free"}', [], False),
    ('demo_memory_usage_bytes{type!="free"}', [], False),
    ('demo_memory_usage_bytes{instance=~"demo.promlabs.com:.*"}', [], False),
    ('demo_memory_usage_bytes{instance=~"host"}', [], False),
    ('demo_memory_usage_bytes{instance!~".*:10000"}', [], False),
    ('demo_memory_usage_bytes{type="free", instance!="demo.promlabs.com:10000"}', [], False),
    ('{type="free", instance!="demo.promlabs.com:10000"}', [], False),
    ('{__name__=~".*"}', [], True),
    ("nonexistent_metric_name", [], False),
    ("demo_memory_usage_bytes offset {{.offset}}", ["offset"], False),
    ("demo_memory_usage_bytes offset -{{.offset}}", ["offset"], False),
    ("demo_intermittent_metric", [], False),

    # Aggregation operators
    ("{{.simpleAggrOp}}(demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}}(nonexistent_metric_name)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} by() (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} by(instance) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} by(instance, type) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} by(nonexistent) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} without() (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} without(instance) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} without(instance, type) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.simpleAggrOp}} without(nonexistent) (demo_memory_usage_bytes)", ["simpleAggrOp"], False),
    ("{{.topBottomOp}} (3, demo_memory_usage_bytes)", ["topBottomOp"], False),
    ("{{.topBottomOp}} by(instance) (2, demo_memory_usage_bytes)", ["topBottomOp"], False),
    ("{{.topBottomOp}} without(instance) (2, demo_memory_usage_bytes)", ["topBottomOp"], False),
    ("{{.topBottomOp}} without() (2, demo_memory_usage_bytes)", ["topBottomOp"], False),
    ("quantile({{.quantile}}, demo_memory_usage_bytes)", ["quantile"], False),
    ("avg(max by(type) (demo_memory_usage_bytes))", [], False),

    # Binary operators
    ("1 * 2 + 4 / 6 - 10 % 2 ^ 2", [], False),
    ("demo_num_cpus + (1 {{.compBinOp}} bool 2)", ["compBinOp"], False),
    ("demo_memory_usage_bytes {{.binOp}} 1.2345", ["binOp"], False),
    ("demo_memory_usage_bytes {{.compBinOp}} bool 1.2345", ["compBinOp"], False),
    ("1.2345 {{.compBinOp}} bool demo_memory_usage_bytes", ["compBinOp"], False),
    ("0.12345 {{.binOp}} demo_memory_usage_bytes", ["binOp"], False),
    ("(1 * 2 + 4 / 6 - (10%7)^2) {{.binOp}} demo_memory_usage_bytes", ["binOp"], False),
    ("demo_memory_usage_bytes {{.binOp}} (1 * 2 + 4 / 6 - 10)", ["binOp"], False),
    ("timestamp(demo_memory_usage_bytes * 1)", [], False),
    ("timestamp(-demo_memory_usage_bytes)", [], False),
    ("demo_memory_usage_bytes {{.binOp}} on(instance, job, type) demo_memory_usage_bytes", ["binOp"], False),
    ("sum by(instance, type) (demo_memory_usage_bytes) {{.binOp}} on(instance, type) group_left(job) demo_memory_usage_bytes", ["binOp"], False),
    ("demo_memory_usage_bytes {{.compBinOp}} bool on(instance, job, type) demo_memory_usage_bytes", ["compBinOp"], False),
    ("demo_memory_usage_bytes / on(instance, job, type, __name__) demo_memory_usage_bytes", [], False),
    ("sum without(job) (demo_memory_usage_bytes) / on(instance, type) demo_memory_usage_bytes", [], False),
    ("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left demo_memory_usage_bytes", [], False),
    ("sum without(job) (demo_memory_usage_bytes) / on(instance, type) group_left(job) demo_memory_usage_bytes", [], False),
    ("demo_memory_usage_bytes / on(instance, job) group_left demo_num_cpus", [], False),
    ("demo_memory_usage_bytes / on(instance, type, job, non_existent) demo_memory_usage_bytes", [], False),

    # NaN/Inf/-Inf support
    ("demo_num_cpus * Inf", [], False),
    ("demo_num_cpus * -Inf", [], False),
    ("demo_num_cpus * NaN", [], False),

    # Unary expressions
    ("demo_memory_usage_bytes + -(1)", [], False),
    ("-demo_memory_usage_bytes", [], False),
    ("-1 ^ 2", [], False),

    # Binops involving non-const scalars
    ("1 {{.arithBinOp}} time()", ["arithBinOp"], False),
    ("time() {{.arithBinOp}} 1", ["arithBinOp"], False),
    ("time() {{.compBinOp}} bool 1", ["compBinOp"], False),
    ("1 {{.compBinOp}} bool time()", ["compBinOp"], False),
    ("time() {{.arithBinOp}} time()", ["arithBinOp"], False),
    ("time() {{.compBinOp}} bool time()", ["compBinOp"], False),
    ("time() {{.binOp}} demo_memory_usage_bytes", ["binOp"], False),
    ("demo_memory_usage_bytes {{.binOp}} time()", ["binOp"], False),

    # Functions: *_over_time
    ("{{.simpleTimeAggrOp}}_over_time(demo_memory_usage_bytes[{{.range}}])", ["simpleTimeAggrOp", "range"], False),
    ("quantile_over_time({{.quantile}}, demo_memory_usage_bytes[{{.range}}])", ["quantile", "range"], False),
    ("timestamp(demo_num_cpus)", [], False),
    ("timestamp(timestamp(demo_num_cpus))", [], False),

    # Functions: math
    ("{{.simpleMathFunc}}(demo_memory_usage_bytes)", ["simpleMathFunc"], False),
    ("{{.simpleMathFunc}}(-demo_memory_usage_bytes)", ["simpleMathFunc"], False),

    # Functions: rate/delta family
    ("{{.extrapolatedRateFunc}}(nonexistent_metric[5m])", ["extrapolatedRateFunc"], False),
    ("{{.extrapolatedRateFunc}}(demo_cpu_usage_seconds_total[{{.range}}])", ["extrapolatedRateFunc", "range"], False),
    ("deriv(demo_disk_usage_bytes[{{.range}}])", ["range"], False),
    ("predict_linear(demo_disk_usage_bytes[{{.range}}], 600)", ["range"], False),

    # Functions: misc
    ("time()", [], False),

    # label_replace
    ('label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "demo.promlabs.com:(.*)")', [], False),
    ('label_replace(demo_num_cpus, "job", "destination-value-$1", "instance", "host:(.*)")', [], False),
    ('label_replace(demo_num_cpus, "job", "$1-$2", "instance", "local(.*):(.*)")', [], False),
    ('label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "source-value-(.*)")', [], False),
    ('label_replace(demo_num_cpus, "job", "value-$1", "nonexistent-src", "(.*)")', [], False),
    ('label_replace(demo_num_cpus, "job", "value-$1", "instance", "non-matching-regex")', [], False),
    ('label_replace(demo_num_cpus, "job", "", "dst", ".*")', [], False),
    ('label_replace(demo_num_cpus, "job", "value-$1", "src", "(.*")', [], True),
    ('label_replace(demo_num_cpus, "~invalid", "", "src", "(.*)")', [], True),
    ('label_replace(demo_num_cpus, "instance", "", "", "")', [], True),

    # label_join
    ('label_join(demo_num_cpus, "new_label", "-", "instance", "job")', [], False),
    ('label_join(demo_num_cpus, "job", "-", "instance", "job")', [], False),
    ('label_join(demo_num_cpus, "job", "-", "instance")', [], False),
    ('label_join(demo_num_cpus, "~invalid", "-", "instance")', [], True),

    # Date functions
    ("{{.dateFunc}}()", ["dateFunc"], False),
    ("{{.dateFunc}}(demo_batch_last_success_timestamp_seconds offset {{.offset}})", ["dateFunc", "offset"], False),

    # Instant rate functions
    ("{{.instantRateFunc}}(demo_cpu_usage_seconds_total[{{.range}}])", ["instantRateFunc", "range"], False),

    # Clamp
    ("{{.clampFunc}}(demo_memory_usage_bytes, 2)", ["clampFunc"], False),
    ("clamp(demo_memory_usage_bytes, 0, 1)", [], False),
    ("clamp(demo_memory_usage_bytes, 0, 1000000000000)", [], False),
    ("clamp(demo_memory_usage_bytes, 1000000000000, 0)", [], False),
    ("clamp(demo_memory_usage_bytes, 1000000000000, 1000000000000)", [], False),

    # Resets / changes
    ("resets(demo_cpu_usage_seconds_total[{{.range}}])", ["range"], False),
    ("changes(demo_batch_last_success_timestamp_seconds[{{.range}}])", ["range"], False),

    # Vector
    ("vector(1.23)", [], False),
    ("vector(time())", [], False),

    # Histogram quantile
    ("histogram_quantile({{.quantile}}, rate(demo_api_request_duration_seconds_bucket[1m]))", ["quantile"], False),
    ("histogram_quantile(0.9, nonexistent_metric)", [], False),
    ("histogram_quantile(0.9, demo_memory_usage_bytes)", [], False),
    ('histogram_quantile(0.9, {__name__=~"demo_api_request_duration_seconds_.+"})', [], False),

    # count_values
    ('count_values("value", demo_api_request_duration_seconds_bucket)', [], False),

    # absent
    ("absent(demo_memory_usage_bytes)", [], False),
    ("absent(nonexistent_metric_name)", [], False),

    # Subqueries
    ("max_over_time((time() - max(demo_batch_last_success_timestamp_seconds) < 1000)[5m:10s] offset 5m)", [], False),
    ("avg_over_time(rate(demo_cpu_usage_seconds_total[1m])[2m:10s])", [], False),
]


def _expand_all_test_cases():
    result = []
    for template, variant_args, should_fail in COMPLIANCE_TEST_CASES:
        for query in _expand_query(template, variant_args):
            result.append((query, should_fail))
    return result


# ── Data ingestion ───────────────────────────────────────────────────────────

def _ingest_openmetrics(data_path):
    """Parse the OpenMetrics file and send via Remote Write to both systems."""
    all_series = parse_openmetrics_file(data_path)
    batch_size = 50
    for i in range(0, len(all_series), batch_size):
        batch = all_series[i : i + batch_size]
        protobuf = convert_time_series_to_protobuf(batch)
        send_protobuf_to_remote_write(
            cluster.prometheus_receiver_ip,
            cluster.prometheus_receiver_port,
            "api/v1/write",
            protobuf,
        )
        send_protobuf_to_remote_write(
            node.ip_address, 9093, "/write", protobuf,
        )


# ── Comparison logic ─────────────────────────────────────────────────────────

def _values_approx_equal(a, b):
    """Same tolerance logic as upstream compliance (EquateApprox + EquateNaNs)."""
    if math.isnan(a) and math.isnan(b):
        return True
    if math.isinf(a) and math.isinf(b):
        return (a > 0) == (b > 0)
    if math.isinf(a) or math.isinf(b) or math.isnan(a) or math.isnan(b):
        return False
    if a == b:
        return True
    return abs(a - b) <= FLOAT_FRACTION * max(abs(a), abs(b)) + FLOAT_MARGIN


def _sort_key(series):
    return tuple(sorted(series.get("metric", {}).items()))


def _compare_scalar(ref, test):
    if len(ref) != 2 or len(test) != 2:
        return False
    if float(ref[0]) != float(test[0]):
        return False
    return _values_approx_equal(float(ref[1]), float(test[1]))


def compare_results(ref_data, test_data):
    """Compare two Prometheus HTTP API data payloads. Returns (ok, diff_msg)."""
    ref = json.loads(ref_data) if isinstance(ref_data, str) else ref_data
    test = json.loads(test_data) if isinstance(test_data, str) else test_data

    ref_type = ref.get("resultType", "")
    test_type = test.get("resultType", "")
    if ref_type != test_type:
        return False, f"resultType mismatch: {ref_type} vs {test_type}"

    ref_result = ref.get("result", [])
    test_result = test.get("result", [])

    if ref_type == "scalar":
        if _compare_scalar(ref_result, test_result):
            return True, ""
        return False, f"scalar mismatch: {ref_result} vs {test_result}"

    if ref_type in ("matrix", "vector"):
        return _compare_series_list(ref_type, ref_result, test_result)

    return False, f"unknown resultType: {ref_type}"


def _compare_series_list(result_type, ref_list, test_list):
    ref_sorted = sorted(ref_list, key=_sort_key)
    test_sorted = sorted(test_list, key=_sort_key)

    if len(ref_sorted) != len(test_sorted):
        return False, f"series count mismatch: {len(ref_sorted)} vs {len(test_sorted)}"

    value_key = "values" if result_type == "matrix" else "value"

    for i, (rs, ts) in enumerate(zip(ref_sorted, test_sorted)):
        if rs.get("metric") != ts.get("metric"):
            return False, f"metric mismatch at [{i}]: {rs.get('metric')} vs {ts.get('metric')}"

        if result_type == "matrix":
            r_vals = rs.get("values", [])
            t_vals = ts.get("values", [])
            if len(r_vals) != len(t_vals):
                return False, f"values count mismatch for {rs.get('metric')}: {len(r_vals)} vs {len(t_vals)}"
            for j, (rv, tv) in enumerate(zip(r_vals, t_vals)):
                if float(rv[0]) != float(tv[0]):
                    return False, f"timestamp mismatch at [{i}][{j}]: {rv[0]} vs {tv[0]}"
                if not _values_approx_equal(float(rv[1]), float(tv[1])):
                    return False, f"value mismatch at [{i}][{j}]: {rv[1]} vs {tv[1]}"
        else:
            rv = rs.get("value", [])
            tv = ts.get("value", [])
            if len(rv) != 2 or len(tv) != 2:
                return False, f"malformed value at [{i}]"
            if float(rv[0]) != float(tv[0]):
                return False, f"timestamp mismatch at [{i}]: {rv[0]} vs {tv[0]}"
            if not _values_approx_equal(float(rv[1]), float(tv[1])):
                return False, f"value mismatch at [{i}]: {rv[1]} vs {tv[1]}"

    return True, ""


def _run_range_query(host, port, path, query):
    """Execute a range query, return (data_json | None, error_string | None)."""
    try:
        response = requests.get(
            f"http://{host}:{port}/{path.strip('/')}",
            params={
                "query": query,
                "start": str(QUERY_START),
                "end": str(QUERY_END),
                "step": str(QUERY_STEP),
            },
            timeout=30,
        )
    except Exception as e:
        return None, str(e)

    try:
        body = response.json()
    except Exception:
        return None, f"HTTP {response.status_code}: {response.text[:200]}"

    status = body.get("status", "")
    if status == "success":
        return json.dumps(body.get("data", {})), None

    error_msg = body.get("error", response.text[:200])
    error_type = body.get("errorType", "")
    return None, f"{error_type}: {error_msg}" if error_type else error_msg


# ── Compliance result tracking ───────────────────────────────────────────────

class ComplianceResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.unsupported = 0
        self.failures = []

    @property
    def total(self):
        return self.passed + self.failed + self.unsupported

    @property
    def score(self):
        return (self.passed / self.total * 100) if self.total > 0 else 0

    def record_pass(self):
        self.passed += 1

    def record_fail(self, query, reason):
        self.failed += 1
        self.failures.append((query, reason))

    def record_unsupported(self, query, reason):
        self.unsupported += 1
        self.failures.append((query, f"UNSUPPORTED: {reason}"))


# ── Fixtures and test ────────────────────────────────────────────────────────

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

        data_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "compliance_data.om"
        )
        generate_openmetrics(data_path)
        _ingest_openmetrics(data_path)

        yield cluster
    finally:
        cluster.shutdown()


def test_promql_compliance():
    """
    Run every query from the Prometheus PromQL compliance test suite against
    both Prometheus (reference) and ClickHouse (test), and report a score.
    """
    test_cases = _expand_all_test_cases()
    result = ComplianceResult()

    for query, should_fail in test_cases:
        ref_data, ref_err = _run_range_query(
            cluster.prometheus_receiver_ip,
            cluster.prometheus_receiver_port,
            "/api/v1/query_range",
            query,
        )
        test_data, test_err = _run_range_query(
            node.ip_address, 9093, "/api/v1/query_range", query,
        )

        ref_failed = ref_err is not None
        test_failed = test_err is not None

        # Reference behaviour doesn't match expectation — skip.
        if ref_failed != should_fail:
            if ref_failed:
                result.record_fail(query, f"reference unexpectedly failed: {ref_err}")
            else:
                result.record_fail(query, "reference unexpectedly succeeded (expected failure)")
            continue

        if should_fail:
            if test_failed:
                result.record_pass()
            else:
                result.record_fail(query, "expected failure but ClickHouse succeeded")
            continue

        if test_failed:
            if "not implemented" in (test_err or "").lower() or "501" in (test_err or ""):
                result.record_unsupported(query, test_err)
            else:
                result.record_fail(query, f"ClickHouse error: {test_err}")
            continue

        match, diff = compare_results(ref_data, test_data)
        if match:
            result.record_pass()
        else:
            result.record_fail(query, diff)

    # ── Print compliance report ──────────────────────────────────────────
    print("\n" + "=" * 80)
    print("PromQL COMPLIANCE REPORT")
    print("=" * 80)
    print(f"Total:       {result.total}")
    print(f"Passed:      {result.passed}")
    print(f"Failed:      {result.failed}")
    print(f"Unsupported: {result.unsupported}")
    print(f"Score:       {result.score:.1f}%")
    print(f"             {result.passed} / {result.total} passed")
    print("=" * 80)

    categories = {}  # key -> [(query, reason)]
    if result.failures:
        import re as _re
        for query, reason in result.failures:
            if "UNSUPPORTED:" in reason:
                m = _re.search(r"(Function \S+ is not implemented|"
                               r"Aggregation operator '\S+' is not implemented|"
                               r"Prometheus query node type \S+ is not implemented|"
                               r"\S+ is not implemented)", reason)
                key = m.group(1) if m else "other unsupported"
            elif "expected failure but ClickHouse succeeded" in reason:
                key = "should_fail mismatch (ClickHouse should reject but accepts)"
            elif "reference unexpectedly" in reason:
                key = "reference mismatch (Prometheus behaves differently than expected)"
            elif "Number of values (0)" in reason:
                key = "aggregation on nonexistent metric errors instead of returning empty"
            elif "Quantile level is out of range" in reason:
                key = "quantile out-of-range phi rejected (Prometheus accepts)"
            elif "value mismatch" in reason or "series count mismatch" in reason:
                key = "result value/count mismatch"
            else:
                key = "other"
            categories.setdefault(key, []).append((query, reason))

        print(f"\n{'─' * 80}")
        print("FAILURE BREAKDOWN BY CATEGORY")
        print(f"{'─' * 80}")
        for cat in sorted(categories, key=lambda c: -len(categories[c])):
            entries = categories[cat]
            print(f"\n  [{len(entries):3d}]  {cat}")
            for q, r in entries[:3]:
                q_short = q if len(q) <= 72 else q[:69] + "..."
                print(f"           e.g. {q_short}")
            if len(entries) > 3:
                print(f"           ... and {len(entries) - 3} more")

            if "other" in cat or "reference" in cat or "mismatch" in cat.split("(")[0].strip():
                print(f"           Details:")
                for q, r in entries:
                    q_short = q if len(q) <= 60 else q[:57] + "..."
                    r_short = r if len(r) <= 100 else r[:97] + "..."
                    print(f"             {q_short}")
                    print(f"               → {r_short}")

    breakdown = {cat: len(entries) for cat, entries in categories.items()}
    out_path = os.environ.get("COMPLIANCE_RESULT_FILE")
    if out_path:
        record = {
            "passed": result.passed,
            "failed": result.failed,
            "unsupported": result.unsupported,
            "total": result.total,
            "pct": round(result.score, 4),
            "breakdown": breakdown,
        }
        with open(out_path, "w") as out_f:
            json.dump(record, out_f, indent=2)
            out_f.write("\n")

    print()
