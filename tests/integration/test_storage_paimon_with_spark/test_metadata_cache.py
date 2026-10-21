import pytest

from helpers.paimon_utils import (
    drop_tables_at_exit,
    get_creation_expression,
    get_uuid_str,
    prepare_simple_paimon_table,
    query_profile_events,
    spark_table,
)

GET_OBJECT_EVENT = {"s3": "S3GetObject", "azure": "AzureGetObject"}


def _get_system_metric_value(instance, metric_name):
    return int(
        instance.query(
            f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
        ).strip()
        or "0"
    )


CACHE_EVENTS = ["PaimonMetadataFilesCacheHits", "PaimonMetadataFilesCacheMisses"]


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_metadata_cache_table_function(started_cluster_paimon_with_spark, storage_type):
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_metadata_cache_tf_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT * FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    # Cold cache: misses.
    hits, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings=cache_on
    )
    assert misses > 0

    # Warm cache: hits only.
    hits, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings=cache_on
    )
    assert misses == 0
    assert hits > 0

    assert _get_system_metric_value(instance, "PaimonMetadataFilesCacheFiles") > 0
    assert _get_system_metric_value(instance, "PaimonMetadataFilesCacheBytes") > 0

    # Explicit drop empties the cache: misses again.
    instance.query("SYSTEM DROP PAIMON METADATA CACHE")
    hits, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings=cache_on
    )
    assert misses > 0

    # Setting off: the cache is not consulted at all.
    hits, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings={"use_paimon_metadata_files_cache": 0}
    )
    assert hits == 0
    assert misses == 0


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_metadata_cache_fewer_object_storage_requests(
    started_cluster_paimon_with_spark, storage_type
):
    """The point of the cache over object storage: a warm query must issue
    strictly fewer GET requests than a cold one."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_metadata_cache_gets_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT count() FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}
    events = [GET_OBJECT_EVENT[storage_type]] + CACHE_EVENTS

    instance.query("SYSTEM DROP PAIMON METADATA CACHE")
    cold_gets, _, cold_misses = query_profile_events(
        instance, select_query, events, settings=cache_on
    )
    warm_gets, warm_hits, _ = query_profile_events(
        instance, select_query, events, settings=cache_on
    )
    assert cold_misses > 0
    assert warm_hits > 0
    assert warm_gets < cold_gets


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_metadata_cache_engine_latched(started_cluster_paimon_with_spark, storage_type):
    """For engine tables use_paimon_metadata_files_cache is latched at CREATE
    time; per-query values are ignored afterwards. Table functions (covered
    above) evaluate it per query."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    table_name = "test_metadata_cache_latched_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    create_query = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=False
    )
    select_query = f"SELECT * FROM {table_name} FORMAT Null"

    # CREATEd with the cache off: per-query setting = 1 has no effect.
    instance.query(
        create_query,
        settings={
            "allow_experimental_paimon_storage_engine": 1,
            "use_paimon_metadata_files_cache": 0,
        },
    )
    with drop_tables_at_exit(instance, table_name):
        for _ in range(2):
            hits, misses = query_profile_events(
                instance,
                select_query,
                CACHE_EVENTS,
                settings={"use_paimon_metadata_files_cache": 1},
            )
            assert hits == 0
            assert misses == 0

    # Re-CREATEd with the cache on: misses, then hits, without any per-query setting.
    instance.query(
        create_query,
        settings={
            "allow_experimental_paimon_storage_engine": 1,
            "use_paimon_metadata_files_cache": 1,
        },
    )
    with drop_tables_at_exit(instance, table_name):
        _, misses = query_profile_events(instance, select_query, CACHE_EVENTS)
        assert misses > 0
        hits, _ = query_profile_events(instance, select_query, CACHE_EVENTS)
        assert hits > 0


def test_metadata_cache_size_bound(started_cluster_paimon_with_spark):
    """node2 runs with paimon_metadata_files_cache_size = 8 KiB — too small to
    hold even one manifest. The bound must be enforced: nothing gets cached."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node2"]
    table_name = "test_metadata_cache_bound_s3_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, "s3", table_name)

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT count() FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    for _ in range(2):
        hits, misses = query_profile_events(
            instance, select_query, CACHE_EVENTS, settings=cache_on
        )
        assert misses > 0
        assert hits == 0


def test_metadata_cache_runtime_resize(started_cluster_paimon_with_spark):
    """paimon_metadata_files_cache_size is changeable without restart: after
    SYSTEM RELOAD CONFIG with a larger size, caching must start working."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node2"]
    config_path = "/etc/clickhouse-server/config.d/paimon_cache_small.xml"
    table_name = "test_metadata_cache_resize_s3_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, "s3", table_name)

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT count() FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    try:
        instance.replace_config(
            config_path,
            "<clickhouse><paimon_metadata_files_cache_size>1073741824"
            "</paimon_metadata_files_cache_size></clickhouse>",
        )
        instance.query("SYSTEM RELOAD CONFIG")

        _, misses = query_profile_events(
            instance, select_query, CACHE_EVENTS, settings=cache_on
        )
        assert misses > 0
        hits, misses = query_profile_events(
            instance, select_query, CACHE_EVENTS, settings=cache_on
        )
        assert misses == 0
        assert hits > 0
    finally:
        instance.replace_config(
            config_path,
            "<clickhouse><paimon_metadata_files_cache_size>8192"
            "</paimon_metadata_files_cache_size></clickhouse>",
        )
        instance.query("SYSTEM RELOAD CONFIG")
        instance.query("SYSTEM DROP PAIMON METADATA CACHE")


def test_metadata_cache_policy_knobs(started_cluster_paimon_with_spark):
    """paimon_metadata_files_cache_policy and ..._size_ratio are applied at
    cache creation (server start): the cache must be fully functional under
    LRU and under SLRU with a non-default protected ratio. Eviction-order
    semantics of the policies are unit-tested at the CacheBase layer
    (gtest_lru_cache / gtest_slru_cache); this pins the knob wiring."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node2"]
    config_path = "/etc/clickhouse-server/config.d/paimon_cache_small.xml"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    configs = (
        (
            "<clickhouse>"
            "<paimon_metadata_files_cache_size>1073741824</paimon_metadata_files_cache_size>"
            "<paimon_metadata_files_cache_policy>LRU</paimon_metadata_files_cache_policy>"
            "</clickhouse>",
            {"paimon_metadata_files_cache_policy": "LRU"},
        ),
        (
            "<clickhouse>"
            "<paimon_metadata_files_cache_size>1073741824</paimon_metadata_files_cache_size>"
            "<paimon_metadata_files_cache_policy>SLRU</paimon_metadata_files_cache_policy>"
            "<paimon_metadata_files_cache_size_ratio>0.3</paimon_metadata_files_cache_size_ratio>"
            "</clickhouse>",
            {
                "paimon_metadata_files_cache_policy": "SLRU",
                "paimon_metadata_files_cache_size_ratio": "0.3",
            },
        ),
    )
    try:
        for config, expected_settings in configs:
            instance.replace_config(config_path, config)
            instance.restart_clickhouse()

            # The knobs must actually be loaded: the warm-cache checks below
            # would also pass if the config plumbing ignored these keys and
            # the server kept its default policy.
            for name, expected_value in expected_settings.items():
                assert (
                    instance.query(
                        f"SELECT value FROM system.server_settings WHERE name = '{name}'"
                    ).strip()
                    == expected_value
                ), name

            table_name = "test_metadata_cache_policy_s3_" + get_uuid_str()
            prepare_simple_paimon_table(started_cluster, "s3", table_name)
            table_expr = get_creation_expression(
                "s3", table_name, started_cluster, table_function=True
            )
            select_query = f"SELECT count() FROM {table_expr} FORMAT Null"

            _, misses = query_profile_events(
                instance, select_query, CACHE_EVENTS, settings=cache_on
            )
            assert misses > 0
            hits, misses = query_profile_events(
                instance, select_query, CACHE_EVENTS, settings=cache_on
            )
            assert misses == 0
            assert hits > 0
    finally:
        instance.replace_config(
            config_path,
            "<clickhouse><paimon_metadata_files_cache_size>8192"
            "</paimon_metadata_files_cache_size></clickhouse>",
        )
        instance.restart_clickhouse()


def test_metadata_cache_eviction(started_cluster_paimon_with_spark):
    """paimon_metadata_files_cache_max_entries is enforced and evictions are
    accounted in PaimonMetadataFilesCacheWeightLost: one table produces several
    cache entries (manifest list + manifests), so with a single-entry cache
    every query keeps evicting and nothing is effectively retained."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node2"]
    config_path = "/etc/clickhouse-server/config.d/paimon_cache_small.xml"
    table_name = "test_metadata_cache_evict_s3_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, "s3", table_name)

    table_expr = get_creation_expression(
        "s3", table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT count() FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    def weight_lost():
        return int(
            instance.query(
                "SELECT value FROM system.events "
                "WHERE event = 'PaimonMetadataFilesCacheWeightLost'"
            ).strip()
            or "0"
        )

    try:
        instance.replace_config(
            config_path,
            "<clickhouse>"
            "<paimon_metadata_files_cache_size>1073741824</paimon_metadata_files_cache_size>"
            "<paimon_metadata_files_cache_max_entries>1</paimon_metadata_files_cache_max_entries>"
            "</clickhouse>",
        )
        instance.query("SYSTEM RELOAD CONFIG")

        weight_lost_before = weight_lost()
        for _ in range(2):
            _, misses = query_profile_events(
                instance, select_query, CACHE_EVENTS, settings=cache_on
            )
            assert misses > 0  # the entry limit prevents effective caching
        assert weight_lost() > weight_lost_before
    finally:
        instance.replace_config(
            config_path,
            "<clickhouse><paimon_metadata_files_cache_size>8192"
            "</paimon_metadata_files_cache_size></clickhouse>",
        )
        instance.query("SYSTEM RELOAD CONFIG")
        instance.query("SYSTEM DROP PAIMON METADATA CACHE")


@pytest.mark.parametrize("storage_type", ["s3", "azure"])
def test_metadata_cache_table_recreation(
    started_cluster_paimon_with_spark, storage_type
):
    """Table-function path only: DROP + re-CREATE of the upstream Paimon table
    at the same path yields a different cache key (the key prefix includes
    schema-0 timeMillis), so a warm cache for the old table must not serve the
    new one. The persistent-engine path latches the table identity and raises a
    logical error ("Underlying Paimon table was recreated") when it changes —
    that aborts sanitizer builds, so it is deliberately not exercised here."""
    started_cluster = started_cluster_paimon_with_spark
    instance = started_cluster.instances["node1"]
    spark = started_cluster.spark_session
    table_name = "test_metadata_cache_recreate_" + storage_type + "_" + get_uuid_str()

    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    table_expr = get_creation_expression(
        storage_type, table_name, started_cluster, table_function=True
    )
    select_query = f"SELECT count() FROM {table_expr} FORMAT Null"
    cache_on = {"use_paimon_metadata_files_cache": 1}

    # Warm the cache for the original table.
    query_profile_events(instance, select_query, CACHE_EVENTS, settings=cache_on)
    _, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings=cache_on
    )
    assert misses == 0

    # Recreate the upstream table at the same path (schema-0 gets a new timeMillis).
    spark.sql(f"DROP TABLE {spark_table(table_name)}")
    prepare_simple_paimon_table(started_cluster, storage_type, table_name)

    # The warm cache belongs to the old table identity: fresh key -> misses.
    _, misses = query_profile_events(
        instance, select_query, CACHE_EVENTS, settings=cache_on
    )
    assert misses > 0
