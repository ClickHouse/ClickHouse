"""Regression tests for the `get_admin_client` readiness retry.

These tests drive `helpers.kafka.common.get_admin_client` with a stub constructor and need
neither a ClickHouse instance nor a Kafka broker. `KafkaAdminClient` reports a broker that
is not up yet as `NoBrokersAvailable` when its readiness probe is cluster-wide, and as
`NodeNotReadyError` when the probe targets a specific node; a real broker cannot be made to
pick one of the two on demand.

`IncompatibleBrokerVersion` and `UnrecognizedBrokerVersion` come out of the same constructor
but mean a genuine version mismatch rather than a broker that is still starting, so they
must fail fast instead of being retried.
"""

import kafka.errors
import pytest

import helpers.kafka.common as k

# Every failed attempt sleeps one second, so keep the budget small. Three is the smallest
# value that admits two failures followed by a success.
RETRIES = 3

SENTINEL = "admin client"


class FakeKafkaCluster:
    kafka_port = 9092


def failing_ctor(exc_cls, fail_times):
    """A `KafkaAdminClient` stand-in raising `exc_cls` on its first `fail_times` calls."""
    state = {"calls": 0}

    def ctor(**kwargs):
        state["calls"] += 1
        if state["calls"] <= fail_times:
            raise exc_cls()
        return SENTINEL

    return ctor, state


@pytest.mark.parametrize(
    "exc_cls", [kafka.errors.NoBrokersAvailable, kafka.errors.NodeNotReadyError]
)
def test_readiness_errors_are_retried(monkeypatch, exc_cls):
    ctor, state = failing_ctor(exc_cls, RETRIES - 1)
    monkeypatch.setattr(k, "KafkaAdminClient", ctor)

    assert k.get_admin_client(FakeKafkaCluster(), retries=RETRIES) is SENTINEL
    # A broker that answered the first probe would return the client too, so pin the
    # number of attempts: both failures have to have been retried.
    assert state["calls"] == RETRIES


@pytest.mark.parametrize(
    "exc_cls",
    [kafka.errors.IncompatibleBrokerVersion, kafka.errors.UnrecognizedBrokerVersion],
)
def test_version_errors_are_not_retried(monkeypatch, exc_cls):
    ctor, state = failing_ctor(exc_cls, RETRIES)
    monkeypatch.setattr(k, "KafkaAdminClient", ctor)

    with pytest.raises(exc_cls):
        k.get_admin_client(FakeKafkaCluster(), retries=RETRIES)
    # One attempt, so the version mismatch is reported as itself instead of being buried
    # under the exhaustion message a retry loop would raise.
    assert state["calls"] == 1
