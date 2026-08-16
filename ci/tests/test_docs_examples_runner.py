import http.client

from tests.docs_examples import runner
from tests.docs_examples.runner import Example


def make_example(query):
    return Example("function", "test", "", 0, query, "")


def test_external_call_detection_matches_registered_ai_aliases():
    assert make_example("SELECT aiGenerate('x')").calls_external_services
    assert make_example("SELECT AIGenerate('x')").calls_external_services
    assert make_example("SELECT AIClassify('x', ['yes'])").calls_external_services


def test_external_call_detection_ignores_non_ai_functions():
    assert not make_example("SELECT generateUUIDv4()").calls_external_services


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
