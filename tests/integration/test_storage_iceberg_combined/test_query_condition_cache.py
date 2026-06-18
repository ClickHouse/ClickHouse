import pytest


@pytest.mark.parametrize("storage_type", ["s3"])
def test_query_condition_cache(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_query_condition_cache_{storage_type}")
    engine.create_table(table, [("id", "int"), ("val", "string")], format_version=2)
    for i in range(1, 10):
        engine.insert(table, [(i, f"value_{i}")])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query("SYSTEM DROP QUERY CONDITION CACHE")

    filter_condition = "WHERE id % 7 = 0"
    select_query = f"SELECT * FROM {target} {filter_condition} ORDER BY id"
    settings = {
        "use_query_condition_cache": 1,
        "allow_experimental_analyzer": 1,
    }

    query_id_first = f"{table}_first"
    result_first = node.query(select_query, query_id=query_id_first, settings=settings)

    node.query("SYSTEM FLUSH LOGS")

    misses_first = int(
        node.query(
            f"SELECT ProfileEvents['QueryConditionCacheMisses'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_first}' AND type = 'QueryFinish'"
        )
    )
    hits_first = int(
        node.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_first}' AND type = 'QueryFinish'"
        )
    )
    assert misses_first > 0, f"Expected cache misses on first run, got {misses_first}"
    assert hits_first == 0, f"Expected no cache hits on first run, got {hits_first}"

    query_id_second = f"{table}_second"
    result_second = node.query(select_query, query_id=query_id_second, settings=settings)
    assert result_second == result_first

    node.query("SYSTEM FLUSH LOGS")

    hits_second = int(
        node.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_second}' AND type = 'QueryFinish'"
        )
    )
    assert hits_second > 0

    node.query("SYSTEM DROP QUERY CONDITION CACHE")

    query_id_after_drop = f"{table}_after_drop"
    node.query(select_query, query_id=query_id_after_drop, settings=settings)

    node.query("SYSTEM FLUSH LOGS")

    misses_after_drop = int(
        node.query(
            f"SELECT ProfileEvents['QueryConditionCacheMisses'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_after_drop}' AND type = 'QueryFinish'"
        )
    )
    hits_after_drop = int(
        node.query(
            f"SELECT ProfileEvents['QueryConditionCacheHits'] "
            f"FROM system.query_log "
            f"WHERE query_id = '{query_id_after_drop}' AND type = 'QueryFinish'"
        )
    )
    assert misses_after_drop > 0
    assert hits_after_drop == 0


@pytest.mark.parametrize("storage_type", ["s3"])
def test_query_condition_cache_nondeterministic_functions(engine, node, storage_type):
    if not engine.supports_storage(storage_type):
        pytest.skip(f"{engine.name} backend does not support {storage_type}")

    table = engine.unique_table(f"test_qcc_nondeterministic_{storage_type}")
    engine.create_table(table, [("id", "int"), ("val", "string")], format_version=2)
    for i in range(1, 10):
        engine.insert(table, [(i, f"value_{i}")])
    engine.sync(table, storage_type)

    target = engine.clickhouse_read_target(node, table, storage_type)

    node.query("SYSTEM DROP QUERY CONDITION CACHE")

    select_query = f"SELECT * FROM {target} WHERE id = rand() FORMAT Null"
    settings = {
        "use_query_condition_cache": 1,
        "allow_experimental_analyzer": 1,
    }

    node.query(select_query, settings=settings)

    cache_size = int(node.query("SELECT count() FROM system.query_condition_cache"))
    assert cache_size == 0
