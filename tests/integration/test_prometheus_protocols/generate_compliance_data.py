#!/usr/bin/env python3
"""
Generates synthetic Prometheus demo-service metrics in OpenMetrics text format.

The output file contains all metrics referenced by the upstream PromQL compliance
test suite (https://github.com/prometheus/compliance/blob/main/promql/promql-test-queries.yml):

  demo_memory_usage_bytes          gauge     (instance, job, type)
  demo_cpu_usage_seconds_total     counter   (instance, job, mode)
  demo_num_cpus                    gauge     (instance, job)
  demo_disk_usage_bytes            gauge     (instance, job)
  demo_batch_last_success_timestamp_seconds  gauge  (instance, job)
  demo_api_request_duration_seconds_bucket   counter (instance, job, method, le)
  demo_intermittent_metric         gauge     (instance, job)

Data covers a 70-minute window (enough for 1h range queries plus a 10-minute
query evaluation window) at 15-second resolution.

Usage:
    python3 generate_compliance_data.py [output_path]
    # default output: compliance_data.om   in the same directory as this script
"""

import math
import os
import sys


# ── Time parameters ──────────────────────────────────────────────────────────

BASE_TIME = 1700000000
DATA_START = BASE_TIME - 4200  # 1h10m before end
DATA_END = BASE_TIME
DATA_INTERVAL = 15  # seconds between samples

INSTANCES = [
    "demo.promlabs.com:10000",
    "demo.promlabs.com:10001",
    "demo.promlabs.com:10002",
]
JOB = "demo"


# ── Value generators (deterministic, no randomness) ──────────────────────────

def _gauge(t, base, amplitude, period=600):
    return base + amplitude * math.sin(2 * math.pi * t / period)


def _counter(t, rate_per_sec, inst_idx):
    elapsed = t - DATA_START
    return rate_per_sec * elapsed * (1 + 0.05 * math.sin(2 * math.pi * t / 300)) + inst_idx * 0.001 * elapsed


def _histogram_cdf(le_val):
    """Fraction of requests with duration <= le (exponential distribution)."""
    if le_val == float("inf"):
        return 1.0
    return 1.0 - math.exp(-le_val * 5)


def _timestamps():
    ts = []
    t = DATA_START
    while t <= DATA_END:
        ts.append(t)
        t += DATA_INTERVAL
    return ts


# ── OpenMetrics formatting ───────────────────────────────────────────────────

def _format_labels(labels):
    """Format a dict of labels as OpenMetrics label list: {k1="v1",k2="v2"}."""
    if not labels:
        return ""
    parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return "{" + parts + "}"


def _format_value(v):
    if math.isinf(v):
        return "+Inf" if v > 0 else "-Inf"
    if math.isnan(v):
        return "NaN"
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(v)


def generate(output_path):
    timestamps = _timestamps()
    lines = []

    def emit_type(name, mtype):
        lines.append(f"# TYPE {name} {mtype}")

    def emit_sample(name, labels, value, ts):
        lines.append(f"{name}{_format_labels(labels)} {_format_value(value)} {ts}")

    # ── demo_memory_usage_bytes (gauge) ──────────────────────────────────
    emit_type("demo_memory_usage_bytes", "gauge")
    mem_types = [
        ("free",    1e9,  1e8),
        ("buffers", 1e8,  1e7),
        ("cached",  5e8,  5e7),
    ]
    for idx, inst in enumerate(INSTANCES):
        for mem_type, base_val, amplitude in mem_types:
            base = base_val + idx * 1e7
            amp = amplitude + idx * 1e6
            for t in timestamps:
                v = _gauge(t, base, amp, 600 + idx * 50)
                emit_sample("demo_memory_usage_bytes",
                            {"instance": inst, "job": JOB, "type": mem_type}, v, t)

    # ── demo_cpu_usage_seconds_total (counter) ───────────────────────────
    emit_type("demo_cpu_usage_seconds_total", "counter")
    cpu_modes = [
        ("idle",   0.9),
        ("user",   0.08),
        ("system", 0.02),
    ]
    for idx, inst in enumerate(INSTANCES):
        for mode, rate in cpu_modes:
            for t in timestamps:
                v = _counter(t, rate, idx)
                emit_sample("demo_cpu_usage_seconds_total",
                            {"instance": inst, "job": JOB, "mode": mode}, v, t)

    # ── demo_num_cpus (gauge, constant) ──────────────────────────────────
    emit_type("demo_num_cpus", "gauge")
    for inst in INSTANCES:
        for t in timestamps:
            emit_sample("demo_num_cpus",
                        {"instance": inst, "job": JOB}, 4.0, t)

    # ── demo_disk_usage_bytes (gauge, slowly increasing) ─────────────────
    emit_type("demo_disk_usage_bytes", "gauge")
    for idx, inst in enumerate(INSTANCES):
        for t in timestamps:
            v = 5e10 + (t - DATA_START) * 1000 + idx * 1e8
            emit_sample("demo_disk_usage_bytes",
                        {"instance": inst, "job": JOB}, v, t)

    # ── demo_batch_last_success_timestamp_seconds (gauge) ────────────────
    emit_type("demo_batch_last_success_timestamp_seconds", "gauge")
    for idx, inst in enumerate(INSTANCES):
        batch_ts = float(DATA_START)
        for t in timestamps:
            if (t - DATA_START) % 300 < DATA_INTERVAL:
                batch_ts = float(t)
            emit_sample("demo_batch_last_success_timestamp_seconds",
                        {"instance": inst, "job": JOB}, batch_ts, t)

    # ── demo_api_request_duration_seconds_bucket (histogram counter) ─────
    emit_type("demo_api_request_duration_seconds_bucket", "counter")
    le_values = ["0.001", "0.01", "0.1", "0.5", "1", "5", "10", "+Inf"]
    for idx, inst in enumerate(INSTANCES):
        for method in ["GET", "POST"]:
            base_rate = 10.0 if method == "GET" else 3.0
            for le_str in le_values:
                le_val = float("inf") if le_str == "+Inf" else float(le_str)
                fraction = _histogram_cdf(le_val)
                for t in timestamps:
                    total = _counter(t, base_rate + idx * 0.1, idx)
                    emit_sample("demo_api_request_duration_seconds_bucket",
                                {"instance": inst, "job": JOB,
                                 "le": le_str, "method": method},
                                total * fraction, t)

    # ── demo_intermittent_metric (gauge with gaps) ───────────────────────
    emit_type("demo_intermittent_metric", "gauge")
    for idx, inst in enumerate(INSTANCES):
        for t in timestamps:
            cycle = (t - DATA_START) % 120
            if cycle < 60:
                v = _gauge(t, 100, 20, 300)
                emit_sample("demo_intermittent_metric",
                            {"instance": inst, "job": JOB}, v, t)

    lines.append("# EOF")

    with open(output_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    sample_count = sum(1 for l in lines if l and not l.startswith("#"))
    series_count = len({
        l.split(" ")[0] for l in lines if l and not l.startswith("#")
    })
    print(f"Generated {output_path}: {sample_count} samples across {series_count} series, "
          f"time range [{DATA_START}, {DATA_END}]")
    return output_path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "compliance_data.om"
    )
    generate(out)
