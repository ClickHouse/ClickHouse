import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
initiator = cluster.add_instance("initiator")
source = cluster.add_instance("source")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        source.query(
            "CREATE TABLE default.squality_source (k UInt64, part UInt64, v String) "
            "ENGINE = Memory"
        )
        source.query("INSERT INTO default.squality_source VALUES (1, 2, 'found')")
        source.query(
            "CREATE USER squality_reader IDENTIFIED WITH no_password "
            "SETTINGS in_null_as_unknown = 1"
        )
        source.query("GRANT SELECT ON default.squality_source TO squality_reader")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "layout,lifetime",
    [
        ("COMPLEX_KEY_DIRECT()", ""),
        ("COMPLEX_KEY_CACHE(SIZE_IN_CELLS 16)", " LIFETIME(30)"),
    ],
)
def test_remote_source_user_profile(layout, lifetime):
    assert (
        initiator.query(
            "SELECT value FROM system.settings WHERE name = 'in_null_as_unknown'"
        )
        == "0\n"
    )
    initiator.query("DROP DICTIONARY IF EXISTS default.squality_remote_dict")
    try:
        initiator.query(
            "CREATE DICTIONARY default.squality_remote_dict "
            "(k UInt64, part UInt64, v String DEFAULT 'missing') "
            "PRIMARY KEY k, part "
            f"SOURCE(CLICKHOUSE(HOST '{source.ip_address}' PORT 9000 "
            "USER 'squality_reader' PASSWORD '' DB 'default' TABLE 'squality_source')) "
            f"LAYOUT({layout}){lifetime}"
        )
        assert (
            initiator.query(
                "SELECT dictGet('default.squality_remote_dict', 'v', (toUInt64(1), toUInt64(2)))"
            )
            == "found\n"
        )
    finally:
        initiator.query("DROP DICTIONARY IF EXISTS default.squality_remote_dict")
