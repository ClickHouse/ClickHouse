from tests.docs_examples.runner import Example


def make_example(query):
    return Example("function", "test", "", 0, query, "")


def test_external_call_detection_matches_registered_ai_aliases():
    assert make_example("SELECT aiGenerate('x')").calls_external_services
    assert make_example("SELECT AIGenerate('x')").calls_external_services
    assert make_example("SELECT AIClassify('x', ['yes'])").calls_external_services


def test_external_call_detection_ignores_non_ai_functions():
    assert not make_example("SELECT generateUUIDv4()").calls_external_services
