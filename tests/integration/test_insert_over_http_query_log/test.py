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


@pytest.mark.parametrize("inject_failpoint", [1, 0])
def test_insert_over_http_exception(start_cluster, inject_failpoint):
    instance.query("DROP TABLE IF EXISTS tt SYNC")
    instance.query(
        "CREATE TABLE tt (KeyID UInt32) Engine = ReplicatedMergeTree('/test_insert_exception_over_http/tt', 'r1') ORDER BY (KeyID)"
    )
    if inject_failpoint > 0:
        instance.query(
            "SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"
        )

    log_comment = f"{inject_failpoint}_02988_66a57d6f-d1cc-4693-8bf4-206848edab87"
    assert True == instance.http_query_and_get_error(
        f"insert into tt settings insert_keeper_max_retries=0, insert_keeper_fault_injection_probability=1.0, log_comment='{log_comment}' values (1), (2), (3), (4), (5)",
        method="POST",
    ).startswith("500 Internal Server Error")

    assert "0\n" == instance.query("select count() from tt")

    instance.query("SYSTEM FLUSH LOGS")

    assert "1\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday() and type = 'QueryStart'"
    )
    assert "1\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday() and type = 'ExceptionWhileProcessing'"
    )
    assert "0\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday() and type != 'QueryStart' and type != 'ExceptionWhileProcessing'"
    )

    instance.query("DROP TABLE tt SYNC")


def test_insert_over_http_invalid_statement(start_cluster):
    http_status = 400
    log_comment = f"{http_status}_02988_66a57d6f-d1cc-4693-8bf4-206848edab87"
    assert True == instance.http_query_and_get_error(
        f"insert into settings log_comment='{log_comment}' values (1), (2), (3), (4), (5)",
        method="POST",
    ).startswith(f"{http_status}")

    instance.query("SYSTEM FLUSH LOGS")

    assert f"0\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday()"
    )


def test_insert_over_http_unknown_table(start_cluster):
    http_status = 404
    log_comment = f"{http_status}_02988_66a57d6f-d1cc-4693-8bf4-206848edab87"
    assert True == instance.http_query_and_get_error(
        f"insert into unknown_table settings log_comment='{log_comment}' values (1), (2), (3), (4), (5)",
        method="POST",
    ).startswith(f"{http_status}")

    instance.query("SYSTEM FLUSH LOGS")

    assert f"1\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday() and type = 'ExceptionBeforeStart'"
    )
    assert f"0\n" == instance.query(
        f"select count() from system.query_log where log_comment ='{log_comment}' and current_database = currentDatabase() and event_date >= yesterday() and type != 'ExceptionBeforeStart'"
    )


def test_insert_over_http_ok(start_cluster):
    instance.query("DROP TABLE IF EXISTS tt SYNC")
    instance.query(
        "CREATE TABLE tt (KeyID UInt32) Engine = ReplicatedMergeTree('/test_insert_exception_over_http/tt', 'r1') ORDER BY (KeyID)"
    )

    _, error = instance.http_query_and_get_answer_with_error(
        "insert into tt settings log_comment='02988_66a57d6f-d1cc-4693-8bf4-206848edab87' values (1), (2), (3), (4), (5)",
        method="POST",
    )
    assert error == None

    assert "5\n" == instance.query("select count() from tt")

    instance.query("SYSTEM FLUSH LOGS")

    assert "1\n" == instance.query(
        "select count() from system.query_log where log_comment ='02988_66a57d6f-d1cc-4693-8bf4-206848edab87' and current_database = currentDatabase() and event_date >= yesterday() and type = 'QueryStart'"
    )

    assert "1\n" == instance.query(
        "select count() from system.query_log where log_comment ='02988_66a57d6f-d1cc-4693-8bf4-206848edab87' and current_database = currentDatabase() and event_date >= yesterday() and type = 'QueryFinish'"
    )

    instance.query("DROP TABLE tt SYNC")
