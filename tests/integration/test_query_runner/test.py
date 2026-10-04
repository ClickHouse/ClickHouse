import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node_query_runner = cluster.add_instance(
    "node_query_runner",
    main_configs=["configs/remote_servers.xml"],
)
node_cluster = cluster.add_instance(
    "node_cluster",
    user_configs=["configs/cluster_user.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def runner_ddl(columns, mode, location):
    cluster_setting = ", cluster = 'qr_cluster', shard = '1'" if location == "cluster" else ""
    return (
        f"CREATE OR REPLACE TABLE runner ({columns}) "
        f"ENGINE = QueryRunner SETTINGS mode = '{mode}'{cluster_setting}"
    )


@pytest.mark.parametrize("mode", ["synchronous", "asynchronous"])
@pytest.mark.parametrize("location, node_target", [("local", node_query_runner), ("cluster", node_cluster)])
def test_dispatching(mode, location, node_target):
    fail_marker = f"qr_dispatch_fail_{mode}_{location}"
    node_target.query("CREATE OR REPLACE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node_target.query("CREATE DATABASE IF NOT EXISTS routed")
    node_target.query("CREATE OR REPLACE TABLE routed.t (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node_query_runner.query(runner_ddl("query String, database String", mode, location))
    node_query_runner.query(
        "INSERT INTO runner VALUES "
        f"('SELECT throwIf(1, ''{fail_marker}'')', ''), "
        "('SELECT 1', ''), "
        "('INSERT INTO default.target VALUES (1)', ''), "
        "('INSERT INTO default.target VALUES (2)', ''), "
        "('INSERT INTO t VALUES (1)', 'routed')"
    )
    assert_eq_with_retry(node_target, "SELECT x FROM target ORDER BY x", "1\n2")
    assert_eq_with_retry(node_target, "SELECT x FROM routed.t", "1")
    node_query_runner.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_query_runner,
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE query LIKE '%{fail_marker}%' AND is_internal AND exception_code != 0",
        "1",
    )


@pytest.mark.parametrize("case", ["definer", "invoker", "none"])
def test_sql_security(case):
    user = f"{case}_user"
    role = f"{case}_role"
    log_comment = f"qr_security_{case}"
    node_query_runner.query(f"CREATE USER {user}")
    node_query_runner.query("CREATE OR REPLACE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
    create_runner = runner_ddl("query String, settings Map(String, String)", "synchronous", "local")
    insert = (
        "INSERT INTO runner VALUES "
        f"('INSERT INTO default.target VALUES (1)', {{'log_comment': '{log_comment}'}})"
    )

    if case == "definer":
        node_query_runner.query(create_runner + f" DEFINER = {user} SQL SECURITY DEFINER")
        inserter = "default"
        attribution = f"{user}\t{user}\tdefault"
    if case == "invoker":
        node_query_runner.query(create_runner + " SQL SECURITY INVOKER")
        node_query_runner.query(f"GRANT INSERT ON default.runner TO {user}")
        inserter = user
        attribution = f"{user}\t{user}\t{user}"
    if case == "none":
        node_query_runner.query(create_runner + " SQL SECURITY NONE")
        node_query_runner.query(f"GRANT INSERT ON default.runner TO {user}")
        inserter = user
        attribution = f"\t\t{user}"

    if case != "none":
        # INSERT on target is granted only through a role.
        node_query_runner.query(insert, user=inserter)
        assert node_query_runner.query("SELECT count() FROM target") == "0\n"
        node_query_runner.query("SYSTEM FLUSH LOGS query_log")
        assert_eq_with_retry(
            node_query_runner,
            f"SELECT count() > 0 FROM system.query_log "
            f"WHERE log_comment = '{log_comment}' AND is_internal AND exception_code = 497",
            "1",
        )
        node_query_runner.query(f"CREATE ROLE {role}")
        node_query_runner.query(f"GRANT INSERT ON default.target TO {role}")
        node_query_runner.query(f"GRANT {role} TO {user}")

    node_query_runner.query(insert, user=inserter)
    assert node_query_runner.query("SELECT count() FROM target") == "1\n"
    node_query_runner.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_query_runner,
        f"SELECT user, initial_user, authenticated_user FROM system.query_log "
        f"WHERE log_comment = '{log_comment}' AND is_internal AND type = 'QueryFinish' LIMIT 1",
        attribution,
    )


def test_cluster_query_log():
    node_query_runner.query(runner_ddl("query String, settings Map(String, String)", "synchronous", "cluster"))
    node_query_runner.query(
        "INSERT INTO runner VALUES ('SELECT 1', {'log_comment': 'qr_cluster_log'})"
    )
    node_query_runner.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_query_runner,
        "SELECT is_initial_query, user, initial_user, authenticated_user FROM system.query_log "
        "WHERE log_comment = 'qr_cluster_log' AND is_internal AND type = 'QueryFinish' LIMIT 1",
        "1\t\t\tdefault",
    )
    node_cluster.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_cluster,
        "SELECT is_initial_query, user, initial_user, authenticated_user, client_name FROM system.query_log "
        "WHERE log_comment = 'qr_cluster_log' AND NOT is_internal AND type = 'QueryFinish' LIMIT 1",
        "1\tcluster_user\tcluster_user\tcluster_user\tClickHouse QueryRunner",
    )


def test_cluster_insert_requires_remote():
    node_query_runner.query(runner_ddl("query String", "synchronous", "cluster"))
    node_query_runner.query("CREATE USER no_remote_user")
    node_query_runner.query("GRANT INSERT ON default.runner TO no_remote_user")
    assert "ACCESS_DENIED" in node_query_runner.query_and_get_error(
        "INSERT INTO runner VALUES ('SELECT 1')", user="no_remote_user"
    )
    node_query_runner.query("GRANT READ ON REMOTE TO no_remote_user")
    assert "ACCESS_DENIED" in node_query_runner.query_and_get_error(
        "INSERT INTO runner VALUES ('SELECT 1')", user="no_remote_user"
    )
    node_query_runner.query("GRANT WRITE ON REMOTE TO no_remote_user")
    node_query_runner.query("INSERT INTO runner VALUES ('SELECT 1')", user="no_remote_user")
    node_query_runner.query("DROP USER no_remote_user")


def test_shutdown():
    # DETACH must cancel an in-flight cluster query and leave no source terminal log.
    marker = "qr_shutdown_cancel"
    node_query_runner.query(runner_ddl("query String", "asynchronous", "cluster"))
    node_query_runner.query(
        "INSERT INTO runner VALUES "
        f"('SELECT sleepEachRow(3) FROM numbers(100500) SETTINGS max_block_size = 1, log_comment = ''{marker}''')"
    )
    in_flight = f"SELECT count() > 0 FROM system.processes WHERE Settings['log_comment'] = '{marker}'"
    assert_eq_with_retry(node_cluster, in_flight, "1")
    node_query_runner.query("DETACH TABLE runner")
    assert_eq_with_retry(node_cluster, in_flight, "0")

    node_cluster.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_cluster,
        f"SELECT count() FROM system.query_log "
        f"WHERE log_comment = '{marker}' AND NOT is_internal "
        f"AND type = 'ExceptionWhileProcessing' AND exception_code = 735",
        "1",
    )
    node_query_runner.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node_query_runner,
        f"SELECT count() FROM system.query_log "
        f"WHERE log_comment = '{marker}' AND is_internal "
        f"AND type IN ('QueryFinish', 'ExceptionWhileProcessing')",
        "0",
    )
    node_query_runner.query("ATTACH TABLE runner")
    node_query_runner.query("DROP TABLE runner")


def test_definer_dependency():
    node_query_runner.query("CREATE USER query_runner_definer")
    node_query_runner.query("CREATE DATABASE qr_definer_db")
    node_query_runner.query(
        "CREATE TABLE qr_definer_db.runner (query String) "
        "ENGINE = QueryRunner SETTINGS mode = 'synchronous' "
        "DEFINER = query_runner_definer SQL SECURITY DEFINER"
    )
    assert "HAVE_DEPENDENT_OBJECTS" in node_query_runner.query_and_get_error("DROP USER query_runner_definer")
    node_query_runner.query("DETACH TABLE qr_definer_db.runner")
    assert "HAVE_DEPENDENT_OBJECTS" in node_query_runner.query_and_get_error("DROP USER query_runner_definer")
    node_query_runner.query("ATTACH TABLE qr_definer_db.runner")
    assert "HAVE_DEPENDENT_OBJECTS" in node_query_runner.query_and_get_error("DROP USER query_runner_definer")
    node_query_runner.query("RENAME TABLE qr_definer_db.runner TO qr_definer_db.runner_renamed")
    assert "HAVE_DEPENDENT_OBJECTS" in node_query_runner.query_and_get_error("DROP USER query_runner_definer")
    node_query_runner.query("RENAME DATABASE qr_definer_db TO qr_definer_db_renamed")
    assert "HAVE_DEPENDENT_OBJECTS" in node_query_runner.query_and_get_error("DROP USER query_runner_definer")
    node_query_runner.query("DROP TABLE qr_definer_db_renamed.runner_renamed SYNC")
    node_query_runner.query("DROP USER query_runner_definer")
    node_query_runner.query("DROP DATABASE qr_definer_db_renamed")


def test_definer_not_tracked_without_table_uuid():
    node_query_runner.query("CREATE USER nil_definer")
    node_query_runner.query("CREATE DATABASE qr_nil_db ENGINE = Memory")
    node_query_runner.query(
        "CREATE TABLE qr_nil_db.runner (query String) ENGINE = QueryRunner "
        "SETTINGS mode = 'synchronous' DEFINER = nil_definer SQL SECURITY DEFINER"
    )
    node_query_runner.query("CREATE VIEW qr_nil_db.v DEFINER = nil_definer SQL SECURITY DEFINER AS SELECT 1")
    node_query_runner.query("DROP USER nil_definer")
    node_query_runner.query("DROP DATABASE qr_nil_db")


def test_wait_query_runner():
    node_query_runner.query("CREATE OR REPLACE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY tuple()")
    node_query_runner.query(runner_ddl("query String", "asynchronous", "local"))
    node_query_runner.query(
        "INSERT INTO runner VALUES "
        "('INSERT INTO default.target SELECT 1 + sleep(0.5)'), "
        "('INSERT INTO default.target SELECT 2 + sleep(0.5)'), "
        "('INSERT INTO default.target SELECT 3 + sleep(0.5)')"
    )
    node_query_runner.query("SYSTEM WAIT QUERY RUNNER runner")
    assert node_query_runner.query("SELECT x FROM target ORDER BY x") == "1\n2\n3\n"
    node_query_runner.query("DROP TABLE runner")
