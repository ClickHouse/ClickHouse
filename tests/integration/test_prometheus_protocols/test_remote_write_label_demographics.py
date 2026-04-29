"""
Prometheus remote-write v1 integration vet matrix for ClickHouse.

Sends multiple WriteRequest payloads (separate HTTP POSTs and one combined-batch
failure case) to exercise:

- Valid lexicographically sorted labels + samples (stored, 204).
- Valid sorted labels + zero samples (silent skip per series, 204).
- Valid sorted + native histogram only, no samples (silent skip, 204).
- Unsorted label names in a request of only bad series (non-204).
- **Same WriteRequest** mixing a valid sorted series and an unsorted series:
  the whole request fails (non-204) and *neither* series is inserted.
- Duplicate label names, missing ``__name__``, empty label name (non-204).
- Exemplar-only time series (no samples): silent skip, 204.

Spec reference: https://prometheus.io/docs/specs/prw/remote_write_spec/ (Labels).

Synthetic metric names use prefix ``itest_rw_vet_`` to avoid collisions.
"""

import os
import sys

import pytest
import requests

from helpers.cluster import ClickHouseCluster

sys.path.insert(1, os.path.join(os.path.dirname(os.path.realpath(__file__)), "pb2"))
from prompb import remote_pb2, types_pb2

from .prometheus_test_utils import get_response_to_remote_write


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def _append_sorted_labels(ts: types_pb2.TimeSeries, labels: dict):
    """Append labels in strict lexicographic order by name (PRW v1)."""
    for name in sorted(labels.keys()):
        lab = ts.labels.add()
        lab.name = name
        lab.value = labels[name]


def _append_unsorted_job_name_instance(ts: types_pb2.TimeSeries, metric_name: str):
    """
    Deliberately wrong order: ``job`` then ``__name__`` then ``instance``.
    Correct sorted order is ``__name__``, ``instance``, ``job``.
    """
    ts.labels.add(name="job", value="itest_unsorted_job")
    ts.labels.add(name="__name__", value=metric_name)
    ts.labels.add(name="instance", value="localhost")


def _count_tags_for_metric(metric_name: str) -> int:
    return int(
        node.query(
            f"SELECT count() FROM timeSeriesTags(prometheus) WHERE metric_name = {repr(metric_name)}"
        ).strip()
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE IF NOT EXISTS prometheus ENGINE=TimeSeries")
        yield cluster
    finally:
        cluster.shutdown()


def _assert_204(resp):
    assert resp.status_code == requests.codes.no_content, (
        f"expected 204, got {resp.status_code}: {resp.text[:500]}"
    )


def _assert_rejected(resp):
    assert resp.status_code != requests.codes.no_content
    assert resp.status_code >= 400, (
        f"expected 4xx/5xx for invalid payload, got {resp.status_code}: {resp.text[:500]}"
    )


# ── Focused tests (optional granularity for CI reports) ─────────────────────


def test_remote_write_mixed_batch_204_zero_sample_series_silently_dropped():
    kept = "itest_rw_vet_mixed_kept"
    dropped = "itest_rw_vet_mixed_zero_samples"

    wr = remote_pb2.WriteRequest()
    ts_kept = wr.timeseries.add()
    _append_sorted_labels(
        ts_kept,
        {"__name__": kept, "instance": "a", "job": "itest"},
    )
    ts_kept.samples.add(timestamp=1_700_000_000_000, value=1.0)

    ts_empty = wr.timeseries.add()
    _append_sorted_labels(
        ts_empty,
        {"__name__": dropped, "instance": "b", "job": "itest"},
    )

    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric(kept) >= 1
    assert _count_tags_for_metric(dropped) == 0


def test_remote_write_204_all_series_zero_samples_nothing_inserted():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    _append_sorted_labels(
        ts,
        {"__name__": "itest_rw_vet_all_zero_samples", "job": "itest"},
    )
    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_all_zero_samples") == 0


def test_remote_write_204_native_histogram_only_silently_dropped():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    _append_sorted_labels(
        ts,
        {"__name__": "itest_rw_vet_native_histogram_only", "job": "itest"},
    )
    h = ts.histograms.add()
    h.sum = 1.0
    h.schema = 0
    h.timestamp = 1_700_000_000_000
    h.count_int = 1

    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_native_histogram_only") == 0


def test_remote_write_204_exemplar_only_no_samples_silently_dropped():
    """Exemplars are ignored; without samples there is nothing to insert."""
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    _append_sorted_labels(
        ts,
        {"__name__": "itest_rw_vet_exemplar_only", "job": "itest"},
    )
    ex = ts.exemplars.add()
    ex.value = 1.0
    ex.timestamp = 1_700_000_000_000
    ex.labels.add(name="trace_id", value="abc")

    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_exemplar_only") == 0


def test_remote_write_unsorted_only_rejected():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    _append_unsorted_job_name_instance(ts, "itest_rw_vet_unsorted_only")
    ts.samples.add(timestamp=1_700_000_000_000, value=1.0)

    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_unsorted_only") == 0


def test_remote_write_one_request_sorted_plus_unsorted_rejects_entire_batch():
    """
    A single WriteRequest contains one fully valid series and one unsorted series.
    ``checkLabels`` fails on the bad series during ``toBlocks``; no insert runs for either.
    """
    good = "itest_rw_vet_same_batch_sorted_ok"
    bad_name = "itest_rw_vet_same_batch_unsorted"

    wr = remote_pb2.WriteRequest()
    ts_ok = wr.timeseries.add()
    _append_sorted_labels(
        ts_ok,
        {"__name__": good, "instance": "i1", "job": "matrix"},
    )
    ts_ok.samples.add(timestamp=1_700_000_000_001, value=42.0)

    ts_bad = wr.timeseries.add()
    _append_unsorted_job_name_instance(ts_bad, bad_name)
    ts_bad.samples.add(timestamp=1_700_000_000_002, value=2.0)

    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric(good) == 0
    assert _count_tags_for_metric(bad_name) == 0


def test_remote_write_duplicate_label_names_rejected():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    ts.labels.add(name="__name__", value="itest_rw_vet_duplicate_job_label")
    ts.labels.add(name="job", value="a")
    ts.labels.add(name="job", value="b")
    ts.samples.add(timestamp=1_700_000_000_000, value=1.0)

    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_duplicate_job_label") == 0


def test_remote_write_missing_metric_name_rejected():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    ts.labels.add(name="job", value="no_name")
    ts.samples.add(timestamp=1_700_000_000_000, value=1.0)

    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))


def test_remote_write_empty_label_name_rejected():
    wr = remote_pb2.WriteRequest()
    ts = wr.timeseries.add()
    ts.labels.add(name="", value="x")
    ts.labels.add(name="__name__", value="itest_rw_vet_empty_label_name")
    ts.samples.add(timestamp=1_700_000_000_000, value=1.0)

    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr))
    assert _count_tags_for_metric("itest_rw_vet_empty_label_name") == 0


# ── Single orchestrated matrix (multiple payloads in one test) ───────────────


def test_remote_write_multi_payload_vet_matrix():
    """
    Sends several WriteRequests in sequence: healthy multi-series batch, invalid
    batches, then a recovery batch — asserts observable state after each stage.
    """
    ts_ms = 1_700_000_010_000

    # Payload 1 — three valid metrics (sorted labels, samples).
    wr_ok = remote_pb2.WriteRequest()
    for name in (
        "itest_rw_vet_matrix_alpha",
        "itest_rw_vet_matrix_beta",
        "itest_rw_vet_matrix_gamma",
    ):
        ts = wr_ok.timeseries.add()
        _append_sorted_labels(ts, {"__name__": name, "job": "matrix", "run": "1"})
        ts.samples.add(timestamp=ts_ms, value=1.0)
    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr_ok))
    for name in (
        "itest_rw_vet_matrix_alpha",
        "itest_rw_vet_matrix_beta",
        "itest_rw_vet_matrix_gamma",
    ):
        assert _count_tags_for_metric(name) >= 1

    # Payload 2 — unsorted labels only (must not touch DB).
    wr_bad_sort = remote_pb2.WriteRequest()
    ts = wr_bad_sort.timeseries.add()
    _append_unsorted_job_name_instance(ts, "itest_rw_vet_matrix_bad_sort")
    ts.samples.add(timestamp=ts_ms + 1, value=1.0)
    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr_bad_sort))
    assert _count_tags_for_metric("itest_rw_vet_matrix_bad_sort") == 0

    # Payload 3 — one sorted + one unsorted in the *same* request (batch must fail entirely).
    wr_mixed = remote_pb2.WriteRequest()
    ts_g = wr_mixed.timeseries.add()
    _append_sorted_labels(
        ts_g,
        {"__name__": "itest_rw_vet_matrix_mixed_should_not_appear", "job": "matrix"},
    )
    ts_g.samples.add(timestamp=ts_ms + 2, value=3.0)
    ts_u = wr_mixed.timeseries.add()
    _append_unsorted_job_name_instance(ts_u, "itest_rw_vet_matrix_mixed_bad")
    ts_u.samples.add(timestamp=ts_ms + 3, value=4.0)
    _assert_rejected(get_response_to_remote_write(node.ip_address, 9093, "/write", wr_mixed))
    assert _count_tags_for_metric("itest_rw_vet_matrix_mixed_should_not_appear") == 0
    assert _count_tags_for_metric("itest_rw_vet_matrix_mixed_bad") == 0

    # Payload 4 — prove earlier successful metrics still present.
    assert _count_tags_for_metric("itest_rw_vet_matrix_alpha") >= 1

    # Payload 5 — new valid metric after errors (server still accepts good writes).
    wr_recover = remote_pb2.WriteRequest()
    ts_r = wr_recover.timeseries.add()
    _append_sorted_labels(
        ts_r,
        {"__name__": "itest_rw_vet_matrix_recovery", "job": "matrix"},
    )
    ts_r.samples.add(timestamp=ts_ms + 4, value=5.0)
    _assert_204(get_response_to_remote_write(node.ip_address, 9093, "/write", wr_recover))
    assert _count_tags_for_metric("itest_rw_vet_matrix_recovery") >= 1
