import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", with_zookeeper=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_insert_exception_over_http(start_cluster):
    instance.query("DROP TABLE IF EXISTS tt SYNC")
    instance.query(
        "CREATE TABLE tt (KeyID UInt32) Engine = ReplicatedMergeTree('/test_insert_exception_over_http/tt', 'r1') ORDER BY (KeyID)"
    )
    instance.query(
        "SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"
    )

    assert True == instance.http_query_and_get_error(
        "insert into tt settings insert_keeper_max_retries=0, insert_keeper_fault_injection_probability=1.0, log_comment='02988_66a57d6f-d1cc-4693-8bf4-206848edab87' values (1), (2), (3), (4), (5)"
    ).startswith("500 Internal Server Error")

    assert "0\n" == instance.query("select count() from tt")
    instance.query("SYSTEM FLUSH LOGS")
    assert "2\n" == instance.query(
        "select count() from system.query_log where log_comment ='02988_66a57d6f-d1cc-4693-8bf4-206848edab87' and current_database = currentDatabase() and event_date >= yesterday()"
    )

    instance.query("DROP TABLE tt SYNC")
