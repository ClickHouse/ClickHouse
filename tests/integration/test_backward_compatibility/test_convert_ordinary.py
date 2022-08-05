import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    image="yandex/clickhouse-server",
    tag="19.17.8.54",
    stay_alive=True,
    with_zookeeper=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def q(query):
    return node.query(query, settings={"log_queries": 1})


def check_convert_system_db_to_atomic():
    q(
        "CREATE TABLE t(date Date, id UInt32) ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id"
    )
    q("INSERT INTO t VALUES (today(), 1)")
    q("INSERT INTO t SELECT number % 1000, number FROM system.numbers LIMIT 1000000")

    assert "1000001\n" == q("SELECT count() FROM t")
    assert "499999500001\n" == q("SELECT sum(id) FROM t")
    assert "1970-01-01\t1000\t499500000\n1970-01-02\t1000\t499501000\n" == q(
        "SELECT date, count(), sum(id) FROM t GROUP BY date ORDER BY date LIMIT 2"
    )
    q("SYSTEM FLUSH LOGS")

    assert "query_log" in q("SHOW TABLES FROM system")
    assert "part_log" in q("SHOW TABLES FROM system")
    q("SYSTEM FLUSH LOGS")
    assert "1\n" == q("SELECT count() != 0 FROM system.query_log")
    assert "1\n" == q("SELECT count() != 0 FROM system.part_log")

    node.restart_with_latest_version(fix_metadata=True)

    assert "Ordinary" in node.query("SHOW CREATE DATABASE default")
    assert "Atomic" in node.query("SHOW CREATE DATABASE system")
    assert "query_log" in node.query("SHOW TABLES FROM system")
    assert "part_log" in node.query("SHOW TABLES FROM system")
    node.query("SYSTEM FLUSH LOGS")

    assert "query_log_0" in node.query("SHOW TABLES FROM system")
    assert "part_log_0" in node.query("SHOW TABLES FROM system")
    assert "1\n" == node.query("SELECT count() != 0 FROM system.query_log_0")
    assert "1\n" == node.query("SELECT count() != 0 FROM system.part_log_0")
    assert "1970-01-01\t1000\t499500000\n1970-01-02\t1000\t499501000\n" == node.query(
        "SELECT date, count(), sum(id) FROM t GROUP BY date ORDER BY date LIMIT 2"
    )
    assert "INFORMATION_SCHEMA\ndefault\ninformation_schema\nsystem\n" == node.query(
        "SELECT name FROM system.databases ORDER BY name"
    )

    errors_count = node.count_in_log("<Error>")
    assert "0\n" == errors_count or (
        "1\n" == errors_count
        and "1\n" == node.count_in_log("Can't receive Netlink response")
    )
    assert "0\n" == node.count_in_log("<Warning> Database")
    errors_count = node.count_in_log("always include the lines below")
    assert "0\n" == errors_count or (
        "1\n" == errors_count
        and "1\n" == node.count_in_log("Can't receive Netlink response")
    )


def create_some_tables(db):
    node.query("CREATE TABLE {}.t1 (n int) ENGINE=Memory".format(db))
    node.query(
        "CREATE TABLE {}.mt1 (n int) ENGINE=MergeTree order by n".format(db),
    )
    node.query(
        "CREATE TABLE {}.mt2 (n int) ENGINE=MergeTree order by n".format(db),
    )
    node.query(
        "CREATE TABLE {}.rmt1 (n int, m int) ENGINE=ReplicatedMergeTree('/test/rmt1/{}', '1') order by n".format(
            db, db
        ),
    )
    node.query(
        "CREATE TABLE {}.rmt2 (n int, m int) ENGINE=ReplicatedMergeTree('/test/{}/rmt2', '1') order by n".format(
            db, db
        ),
    )
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"sed --follow-symlinks -i 's|/test/{db}/rmt2|/test/{{database}}/{{table}}|' /var/lib/clickhouse/metadata/{db}/rmt2.sql",
        ]
    )
    node.query(
        "CREATE MATERIALIZED VIEW {}.mv1 (n int) ENGINE=ReplicatedMergeTree('/test/{}/mv1/', '1') order by n AS SELECT n FROM {}.rmt1".format(
            db, db, db
        ),
    )
    node.query(
        "CREATE MATERIALIZED VIEW {}.mv2 (n int) ENGINE=MergeTree order by n AS SELECT n FROM {}.rmt2".format(
            db, db
        ),
    )
    node.query(
        "CREATE DICTIONARY {}.d1 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt1' PASSWORD '' DB '{}')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())".format(db, db)
    )
    node.query(
        "CREATE DICTIONARY {}.d2 (n int DEFAULT 0, m int DEFAULT 1) PRIMARY KEY n "
        "SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'rmt2' PASSWORD '' DB '{}')) "
        "LIFETIME(MIN 1 MAX 10) LAYOUT(FLAT())".format(db, db)
    )
    node.query(
        "CREATE TABLE {}.merge (n int) ENGINE=Merge('{}', '(mt)|(mv)')".format(db, db)
    )


def check_convert_all_dbs_to_atomic():
    node.query(
        "CREATE DATABASE ordinary ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        "CREATE DATABASE other ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        "CREATE DATABASE `.o r d i n a r y.` ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query("CREATE DATABASE atomic ENGINE=Atomic")
    node.query("CREATE DATABASE mem ENGINE=Memory")
    node.query("CREATE DATABASE lazy ENGINE=Lazy(1)")

    tables_with_data = ["mt1", "mt2", "rmt1", "rmt2", "mv1", "mv2"]

    for db in ["ordinary", "other", "atomic"]:
        create_some_tables(db)
        for table in tables_with_data:
            node.query("INSERT INTO {}.{} (n) VALUES ({})".format(db, table, len(db)))

    node.query(
        "CREATE TABLE `.o r d i n a r y.`.`t. a. b. l. e.` (n int) ENGINE=MergeTree ORDER BY n"
    )
    node.query("CREATE TABLE lazy.table (n int) ENGINE=Log")

    # Introduce some cross dependencies
    node.query(
        "CREATE TABLE ordinary.l (n DEFAULT dictGet('other.d1', 'm', toUInt64(3))) ENGINE=Log"
    )
    node.query(
        "CREATE TABLE other.l (n DEFAULT dictGet('ordinary.d1', 'm', toUInt64(3))) ENGINE=StripeLog"
    )

    node.query(
        "CREATE TABLE atomic.l (n DEFAULT dictGet('ordinary.d1', 'm', toUInt64(3))) ENGINE=TinyLog"
    )

    tables_without_data = ["t1", "d1", "d2", "merge", "l"]

    # 6 tables + 2 inner tables of MVs, each contains 2 rows
    for db in ["ordinary", "other"]:
        assert "12\t{}\n".format(12 * len(db)) == node.query(
            "SELECT count(), sum(n) FROM {}.merge".format(db)
        )

    # 6 tables, MVs contain 2 rows (inner tables does not match regexp)
    assert "8\t{}\n".format(8 * len("atomic")) == node.query(
        "SELECT count(), sum(n) FROM atomic.merge".format(db)
    )

    node.exec_in_container(
        ["bash", "-c", f"touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.restart_clickhouse()

    assert (
        ".o r d i n a r y.\natomic\ndefault\nordinary\nother\nsystem\n"
        == node.query(
            "SELECT name FROM system.databases WHERE engine='Atomic' ORDER BY name"
        )
    )
    assert "Lazy\nMemory\n" == node.query(
        "SELECT engine FROM system.databases WHERE name IN ('mem', 'lazy') ORDER BY name"
    )
    assert "t. a. b. l. e.\n" == node.query("SHOW TABLES FROM `.o r d i n a r y.`")
    assert "table\n" == node.query("SHOW TABLES FROM lazy")

    for db in ["ordinary", "other", "atomic"]:
        assert "\n".join(
            sorted(tables_with_data + tables_without_data) + [""]
        ) == node.query("SHOW TABLES FROM {} NOT LIKE '%inner%'".format(db))

    for db in ["ordinary", "other"]:
        assert "8\t{}\n".format(8 * len(db)) == node.query(
            "SELECT count(), sum(n) FROM {}.merge".format(db)
        )

    for db in ["ordinary", "other", "atomic"]:
        for table in tables_with_data:
            node.query(
                "INSERT INTO {}.{} (n) VALUES ({})".format(db, table, len(db) * 3)
            )

    for db in ["ordinary", "other", "atomic"]:
        assert "16\t{}\n".format(16 * len(db) * 2) == node.query(
            "SELECT count(), sum(n) FROM {}.merge".format(db)
        )


def test_convert_ordinary_to_atomic(start_cluster):
    check_convert_system_db_to_atomic()
    check_convert_all_dbs_to_atomic()
