import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, main_configs=[])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()

def test_sql_user_defined_functions_cache(started_cluster):
    '''Test that the sql UDFs are non-deterministic by default'''
    # I reuse ch1 only
    node.query_with_retry("DROP FUNCTION IF EXISTS test_function")
    node.query_with_retry("CREATE FUNCTION test_function AS x -> x + 1;")

    # test that when not specified the default behavior is implicitly non-deterministic
    assert node.query_and_get_error("SELECT test_function(1) SETTINGS use_query_cache = true;")
    assert node.query_and_get_error("SELECT test_function(2) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'")
    assert node.query("SELECT count(*) FROM system.query_cache") == "0\n"
    node.query("SYSTEM DROP QUERY CACHE");

    # Test no with 'save'
    assert node.query("SELECT test_function(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save'") == "2\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "1\n"
    node.query("SYSTEM DROP QUERY CACHE");

    # Just ignore.
    assert node.query("SELECT test_function(1) SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore'") == "2\n"
    assert node.query("SELECT count(*) FROM system.query_cache") == "0\n"
    node.query("SYSTEM DROP QUERY CACHE");
