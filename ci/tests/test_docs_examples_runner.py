import http.client
import json

import pytest

from tests.docs_examples import runner
from tests.docs_examples.runner import Example


def make_example(query):
    return Example("function", "test", "", 0, query, "")


class FakeDocumentationClient:
    """A client whose `system.documentation` holds one entity with the given description."""

    def __init__(self, description):
        self.description = description

    def query(self, sql, **params):
        row = {"name": "test", "type": "Function", "description": self.description, "source": "src/test.cpp"}
        return True, json.dumps(row) + "\n"


def test_external_call_detection_matches_registered_ai_aliases():
    assert make_example("SELECT aiGenerate('x')").calls_external_services
    assert make_example("SELECT AIGenerate('x')").calls_external_services
    assert make_example("SELECT AIClassify('x', ['yes'])").calls_external_services


def test_external_call_detection_ignores_non_ai_functions():
    assert not make_example("SELECT generateUUIDv4()").calls_external_services


def test_global_object_detection_matches_generate_serial_id():
    assert make_example("SELECT generateSerialID('id1')").creates_global_objects


def test_global_object_detection_ignores_other_functions():
    assert not make_example("SELECT generateUUIDv4()").creates_global_objects


def test_global_object_detection_sees_through_leading_comments():
    assert make_example("-- note\nCREATE USER u IDENTIFIED WITH no_password").creates_global_objects
    assert make_example("/* note */ CREATE DATABASE db").creates_global_objects
    assert make_example("-- first\n/* second */\nGRANT SELECT ON *.* TO u").creates_global_objects


def test_global_object_detection_does_not_read_the_comment_text():
    assert not make_example("-- CREATE USER appears in a comment only\nSELECT 1").creates_global_objects


def test_load_examples_takes_the_documented_response():
    description = "```sql title=Query\nSELECT 1\n```\n\n```response title=Response\n1\n```\n"
    examples = runner.load_examples(FakeDocumentationClient(description))
    assert len(examples) == 1
    assert examples[0].query == "SELECT 1"
    assert examples[0].result == "1"


def test_load_examples_accepts_an_example_that_documents_no_response():
    description = "```sql title=Query\nSELECT 1\n```\n"
    examples = runner.load_examples(FakeDocumentationClient(description))
    assert len(examples) == 1
    assert examples[0].result == ""


@pytest.mark.parametrize("fence", ["```response title=Unexpected", "```text", "```json", "```sql"])
def test_load_examples_rejects_an_unrecognized_response_fence(fence):
    # If the renderer changes how it emits a response - the spelling of the fence, or the fence type
    # altogether - the run must stop instead of silently downgrading every example from output
    # comparison to "the query runs".
    description = f"```sql title=Query\nSELECT 1\n```\n\n{fence}\n1\n```\n"
    with pytest.raises(RuntimeError, match="does not recognize as its response"):
        runner.load_examples(FakeDocumentationClient(description))


def test_load_examples_accepts_the_query_of_the_next_example_after_a_response_less_one():
    # An example that documents no response is followed by the query of the next example, which is
    # the one fenced block that is not renderer drift.
    description = (
        "```sql title=Query\nSELECT 1\n```\n\n"
        "```sql title=Query\nSELECT 2\n```\n\n```response title=Response\n2\n```\n"
    )
    examples = runner.load_examples(FakeDocumentationClient(description))
    assert [(e.query, e.result) for e in examples] == [("SELECT 1", ""), ("SELECT 2", "2")]


def test_normalize_preserves_tsv_empty_fields_and_blank_rows():
    assert runner.normalize("a\t\nb") != runner.normalize("a\nb")
    assert runner.normalize("a\n\nb") != runner.normalize("a\nb")


def test_normalize_ignores_the_trailing_line_terminator():
    assert runner.normalize("a\n") == runner.normalize("a")


def test_client_does_not_retry_a_request_after_a_transport_error(monkeypatch):
    class FailingConnection:
        requests = 0

        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            self.__class__.requests += 1

        def getresponse(self):
            raise http.client.HTTPException("response lost")

        def close(self):
            pass

    monkeypatch.setattr(runner.http.client, "HTTPConnection", FailingConnection)

    ok, message = runner.Client("localhost", 8123, "default", "", 1).query("INSERT INTO t VALUES (1)")

    assert not ok
    assert message == "Connection error: response lost"
    assert FailingConnection.requests == 1
