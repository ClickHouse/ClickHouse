import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", stay_alive=True, main_configs=["configs/config.d/query_log.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_profile_event_for_query(node, query, profile_event):
    node.query("system flush logs")
    query = query.replace("'", "\\'")
    return int(
        node.query(
            f"select ProfileEvents['{profile_event}'] from system.query_log where query='{query}' and type = 'QueryFinish' order by event_time desc limit 1"
        )
    )


def test(start_cluster):
    node.query("insert into function file('data.jsonl') select * from numbers(100)")
    desc_query = "desc file('data.jsonl')"
    node.query(desc_query)
    cache_misses = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheMisses"
    )
    assert cache_misses == 1

    desc_query = "desc file('data.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1

    node.query("insert into function file('data.jsonl') select * from numbers(100)")
    desc_query = "desc file('data.jsonl')"
    node.query(desc_query)
    cache_invalidations = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheInvalidations"
    )
    assert cache_invalidations == 1

    node.query("insert into function file('data1.jsonl') select * from numbers(100)")
    desc_query = (
        "desc file('data1.jsonl') settings cache_ttl_for_file_schema_inference=1"
    )
    node.query(desc_query)
    cache_misses = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheMisses"
    )
    assert cache_misses == 1

    time.sleep(2)

    desc_query = (
        "desc file('data1.jsonl') settings cache_ttl_for_file_schema_inference=1000"
    )
    node.query(desc_query)
    cache_misses = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheMisses"
    )
    assert cache_misses == 1
    cache_ttl_expirations = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheTTLExpirations"
    )
    assert cache_ttl_expirations == 1

    desc_query = "desc file('data1.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1
    cache_ttl_updates = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheTTLUpdates"
    )
    assert cache_ttl_updates == 1

    node.query("insert into function file('data1.jsonl') select * from numbers(100)")
    desc_query = (
        "desc file('data1.jsonl') settings cache_ttl_for_file_schema_inference=1"
    )
    node.query(desc_query)
    cache_invalidations = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheInvalidations"
    )
    assert cache_invalidations == 1

    time.sleep(2)

    desc_query = "desc file('data.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1
    cache_ttl_expirations = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheTTLExpirations"
    )
    assert cache_ttl_expirations == 1

    desc_query = "desc file('data1.jsonl')"
    node.query(desc_query)
    cache_misses = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheMisses"
    )
    assert cache_misses == 1

    node.query("insert into function file('data2.jsonl') select * from numbers(100)")
    node.query("insert into function file('data3.jsonl') select * from numbers(100)")

    desc_query = "desc file('data*.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1

    desc_query = "desc file('data2.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1

    desc_query = "desc file('data3.jsonl')"
    node.query(desc_query)
    cache_hits = get_profile_event_for_query(
        node, desc_query, "SchemaInferenceCacheHits"
    )
    assert cache_hits == 1
