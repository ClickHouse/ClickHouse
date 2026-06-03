import os
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import get_database_disk_name, write_metadata

from helpers.test_tools import TSV
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml", "configs/backups_disk.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
    external_dirs=["/backups/"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Time series data for "foo" — inserted directly into prealpha inner tables before upgrade.
foo = [({"__name__": "foo", "job": "prometheus"}, {1000.0: 10.0})]

# Time series data for "bar" — inserted via RemoteWrite after upgrade.
bar = [({"__name__": "bar", "job": "prometheus"}, {2000.0: 20.0})]


# DDL for prealpha inner tables schema.
# The "samples" table was named "data".
# The `type` and `unit` columns were plain `String`, not `LowCardinality(String)`.
# The `INNER COLUMNS` clause was not used.
PREALPHA_DATA_DEF = (
    "(id UUID, timestamp DateTime64(3), value Float64)"
    " ENGINE=MergeTree ORDER BY (id, timestamp)"
)

PREALPHA_TAGS_DEF = (
    "(id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),"
    " metric_name LowCardinality(String),"
    " tags Map(LowCardinality(String), String),"
    " all_tags Map(String, String) EPHEMERAL,"
    " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),"
    " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))"
    " ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
)

PREALPHA_METRICS_DEF = (
    "(metric_family_name String, type String, unit String, help String)"
    " ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
)

# Column list as it would appear in the prealpha metadata file.
PREALPHA_COLUMNS = """\
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)"""

NIL_UUID = "00000000-0000-0000-0000-000000000000"


# Creates and fills the prealpha version of TimeSeries table.
# The function builds its inner tables manually, then it writes the prealpha-version metadata,
# and finally attaches the TimeSeries table.
def create_and_fill_prealpha_time_series(time_series_columns=PREALPHA_COLUMNS,
                                          time_series_settings="",
                                          data_def=PREALPHA_DATA_DEF,
                                          tags_def=PREALPHA_TAGS_DEF,
                                          metrics_def=PREALPHA_METRICS_DEF):
    # This dummy table is to detach, rewrite its metadata and attach again.
    node.query("CREATE TABLE prometheus (dummy UInt8) ENGINE=Null")

    ts_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'prometheus'"
    ).strip()

    # Inner table names depend on whether we're in an Atomic database (UUID != nil) or Ordinary.
    # The prealpha version used the `data` kind string for the data inner table (before `samples`).
    if ts_uuid != NIL_UUID:
        data_table_name    = f".inner_id.data.{ts_uuid}"
        tags_table_name    = f".inner_id.tags.{ts_uuid}"
        metrics_table_name = f".inner_id.metrics.{ts_uuid}"
    else:
        data_table_name    = ".inner.data.prometheus"
        tags_table_name    = ".inner.tags.prometheus"
        metrics_table_name = ".inner.metrics.prometheus"

    node.query(f"CREATE TABLE `{data_table_name}`    {data_def}")
    node.query(f"CREATE TABLE `{tags_table_name}`    {tags_def}")
    node.query(f"CREATE TABLE `{metrics_table_name}` {metrics_def}")

    insert_foo_into_prealpha_time_series(data_table_name, tags_table_name, metrics_table_name)

    data_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{data_table_name}'"
    ).strip()
    tags_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{tags_table_name}'"
    ).strip()
    metrics_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{metrics_table_name}'"
    ).strip()

    node.query("DETACH TABLE prometheus")

    prometheus_sql_path = node.query(
        "SELECT metadata_path FROM system.detached_tables WHERE database = 'default' AND table = 'prometheus'"
    ).strip()

    uuid_clause = f"UUID '{ts_uuid}'" if ts_uuid != NIL_UUID else ""
    inner_uuid_clause = ""
    if data_uuid != NIL_UUID:
        inner_uuid_clause += f"DATA INNER UUID '{data_uuid}'\n"
    if tags_uuid != NIL_UUID:
        inner_uuid_clause += f"TAGS INNER UUID '{tags_uuid}'\n"
    if metrics_uuid != NIL_UUID:
        inner_uuid_clause += f"METRICS INNER UUID '{metrics_uuid}'\n"

    settings_clause = f"SETTINGS {time_series_settings}" if time_series_settings else ""

    metadata = (
        f"ATTACH TABLE prometheus {uuid_clause}\n"
        f"{time_series_columns}\n"
        f"ENGINE = TimeSeries\n"
        f"{inner_uuid_clause}"
        f"{settings_clause}\n"
    )
    write_metadata(node, prometheus_sql_path, metadata)

    # We need to clear the cache before ATTACH reads the new metadata
    # (because write_metadata() bypassed the server, so the cached file size is stale).
    db_disk_name = get_database_disk_name(node)
    if db_disk_name != "default":
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")

    node.query("ATTACH TABLE prometheus")

    # The prealpha version's outer columns (`id`, `timestamp`, `value`, ...) must be replaced by the canonical
    # current outer column `time_series Array(Tuple(timestamp, value))` during the load-time normalization.
    outer_columns = set(node.query(
        "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'prometheus'"
    ).split())
    assert "time_series" in outer_columns
    assert "id" not in outer_columns


def insert_foo_into_prealpha_time_series(data_table, tags_table, metrics_table):
    foo_id = node.query(
        "SELECT reinterpretAsUUID(sipHash128('foo', mapSort(map('__name__', 'foo', 'job', 'prometheus'))))"
    ).strip()
    node.query(f"INSERT INTO `{data_table}` VALUES ('{foo_id}', toDateTime64(1000, 3), 10.0)")
    node.query(
        f"INSERT INTO `{tags_table}` (id, metric_name, tags, all_tags)"
        f" VALUES ('{foo_id}', 'foo', {{'job': 'prometheus'}}, mapSort(map('__name__', 'foo', 'job', 'prometheus')))"
    )
    node.query(f"INSERT INTO `{metrics_table}` VALUES ('foo', 'gauge', 'bytes', 'Foo metric')")


# Sends the `bar` metric via RemoteWrite protocol to a table after it's upgraded or restored.
def send_bar_via_remote_write():
    protobuf = convert_time_series_to_protobuf(bar)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


# Checks that both `foo` and `bar` metrics exist.
def check_foo_and_bar():
    result = node.query(
        "SELECT t.metric_name, d.timestamp, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " JOIN timeSeriesTags(prometheus) AS t ON d.id = t.id"
        " ORDER BY t.metric_name, d.timestamp"
    )
    assert result == TSV([
        ["bar", "1970-01-01 00:33:20.000", "20"],
        ["foo", "1970-01-01 00:16:40.000", "10"],
    ])


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS default.prometheus SYNC")


# Checks that an prealpha-version TimeSeries table can be attached and used.
def test_upgrade_from_prealpha():
    create_and_fill_prealpha_time_series()
    send_bar_via_remote_write()
    check_foo_and_bar()


# Checks that an prealpha-version TimeSeries table can be attached and used (Ordinary database).
def test_upgrade_from_prealpha_ordinary_db():
    node.query("DROP DATABASE default SYNC")
    node.query(
        "CREATE DATABASE default ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_and_fill_prealpha_time_series()
    send_bar_via_remote_write()
    check_foo_and_bar()

    node.query("DROP TABLE default.prometheus SYNC")
    node.query("DROP DATABASE default SYNC")
    node.query("CREATE DATABASE default")


# Checks that an prealpha-version TimeSeries table can be restored and used.
def test_restore_from_prealpha():
    backup_file = os.path.join(os.path.dirname(__file__), "backups", "time_series_prealpha.zip")
    node.copy_file_to_container(backup_file, "/backups/time_series_prealpha.zip")
    node.query("RESTORE TABLE default.prometheus FROM Disk('backups', 'time_series_prealpha.zip')")
    send_bar_via_remote_write()
    check_foo_and_bar()
