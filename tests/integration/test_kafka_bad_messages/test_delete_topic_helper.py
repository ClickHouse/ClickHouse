"""Regression tests for the kafka_delete_topic teardown helper.

These tests drive helpers.kafka.common.kafka_delete_topic with a stub admin client and
need neither a ClickHouse instance nor a Kafka broker. A real broker cannot be made to
drop exactly one DeleteTopicsResponse on demand, so the interleaving that broke teardown
in CI is only reproducible deterministically with a stub. The stub replays the exact RPC
sequence recorded in the failing job's client log:

    18:44:18.560  DeleteTopicsRequest_v3(topics=['<topic>'], timeout=30000) sent
    18:44:48.570  BrokerConnection ... timed out after 30000 ms. [Error 7] RequestTimedOutError
    18:44:49.077  MetadataResponse ... ClusterMetadata(brokers: 1, topics: 3, groups: 0)
    18:44:49.078+ DeleteTopicsResponse_v3(topic_error_codes=[(topic='<topic>', error_code=3)]) x49

i.e. the delete succeeded broker-side, its response was lost, and every retry then
reported UnknownTopicOrPartitionError (error_code 3) - "the topic does not exist", which
for a delete is the requested end state.
"""

from types import SimpleNamespace

import kafka.errors
import pytest

import helpers.kafka.common as k

TOPIC = "test_delete_topic_helper_topic"

# Keep the master-side failure fast: the production default of 50 burns ~23 seconds.
MAX_RETRIES = 3


class StubAdminClient:
    """Admin client whose delete_topics replays a scripted sequence of outcomes.

    Each element of `outcomes` is either an exception instance to raise or a
    DeleteTopicsResponse-like object to return. The last element is repeated once the
    script is exhausted.
    """

    def __init__(self, outcomes, listed_topics):
        self.outcomes = outcomes
        self.listed_topics = listed_topics
        self.delete_calls = 0
        self.list_calls = 0

    def delete_topics(self, topics):
        self.delete_calls += 1
        outcome = self.outcomes[min(self.delete_calls, len(self.outcomes)) - 1]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def list_topics(self):
        self.list_calls += 1
        return list(self.listed_topics)


def deleted_response(topic):
    return SimpleNamespace(topic_error_codes=[(topic, 0)])


def test_lost_response_then_topic_absent_is_success():
    """The observed CI interleaving must not fail teardown.

    Attempt 1 loses its response (RequestTimedOutError); every later attempt reports
    error_code 3 because the topic is in fact already gone. Without the fix the helper
    retries error 3 to exhaustion and raises UnknownTopicOrPartitionError out of teardown.
    """
    stub = StubAdminClient(
        outcomes=[
            kafka.errors.RequestTimedOutError(),
            kafka.errors.UnknownTopicOrPartitionError(),
        ],
        listed_topics=["_schemas", "__consumer_offsets"],
    )

    k.kafka_delete_topic(stub, TOPIC, max_retries=MAX_RETRIES)

    # Error 3 is terminal, not retryable: attempt 1 (lost response) plus one attempt that
    # reports the topic as absent. A fix that kept retrying to max_retries and only then
    # gave up would still return, so bound the attempts explicitly.
    assert stub.delete_calls == 2
    # The absent-topic branch must reach the authoritative listing check rather than
    # returning blind.
    assert stub.list_calls >= 1


def test_topic_absent_on_first_attempt_is_success():
    """A delete of an already-absent topic succeeds without retrying.

    This is the routine case for tests/casa_del_dolor/catalogs/kafkatest.py, which deletes
    randomly chosen topic names.
    """
    stub = StubAdminClient(
        outcomes=[kafka.errors.UnknownTopicOrPartitionError()],
        listed_topics=[],
    )

    k.kafka_delete_topic(stub, TOPIC, max_retries=MAX_RETRIES)

    assert stub.delete_calls == 1
    assert stub.list_calls >= 1


def test_topic_still_listed_after_error_3_still_raises():
    """Negative control: the listing check, not error 3, decides success.

    If the broker claims the topic does not exist while still listing it, the helper must
    still fail. This is what makes the fix "trust the listing" instead of "swallow the
    error".
    """
    stub = StubAdminClient(
        outcomes=[kafka.errors.UnknownTopicOrPartitionError()],
        listed_topics=[TOPIC],
    )

    with pytest.raises(Exception):
        k.kafka_delete_topic(stub, TOPIC, max_retries=MAX_RETRIES)


def test_transient_errors_are_still_retried():
    """Genuinely transient RPC failures must keep being retried."""
    stub = StubAdminClient(
        outcomes=[
            kafka.errors.RequestTimedOutError(),
            kafka.errors.RequestTimedOutError(),
            deleted_response(TOPIC),
        ],
        listed_topics=[],
    )

    k.kafka_delete_topic(stub, TOPIC, max_retries=MAX_RETRIES)

    assert stub.delete_calls == 3
    assert stub.list_calls >= 1


def test_transient_errors_are_still_raised_when_exhausted():
    """A permanently failing delete RPC must still fail loudly."""
    stub = StubAdminClient(
        outcomes=[kafka.errors.RequestTimedOutError()],
        listed_topics=[],
    )

    with pytest.raises(kafka.errors.RequestTimedOutError):
        k.kafka_delete_topic(stub, TOPIC, max_retries=MAX_RETRIES)

    assert stub.delete_calls == MAX_RETRIES
