import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", stay_alive=True,
    main_configs=["configs/config.d/query_log.xml", "configs/config.d/schema_cache.xml"]
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


def check_cache_misses(node, file, amount=1):
    assert get_profile_event_for_query(node, f"desc file('{file}')",
                                       "SchemaInferenceCacheMisses") == amount


def check_cache_hits(node, file, amount=1):
    assert get_profile_event_for_query(node, f"desc file('{file}')",
                                       "SchemaInferenceCacheHits") == amount


def check_cache_invalidations(node, file, amount=1):
    assert get_profile_event_for_query(node, f"desc file('{file}')",
                                       "SchemaInferenceCacheInvalidations") == amount


def check_cache_evictions(node, file, amount=1):
    assert get_profile_event_for_query(node, f"desc file('{file}')",
                                       "SchemaInferenceCacheEvictions") == amount


def check_cache(node, expected_files):
    sources = node.query("select source from system.schema_inference_cache")
    assert sorted(map(lambda x: x.strip().split("/")[-1], sources.split())) == sorted(expected_files)


def test(start_cluster):
    node.query("insert into function file('data.jsonl') select * from numbers(100)")
    time.sleep(1)

    node.query("desc file('data.jsonl')")
    check_cache(node, ["data.jsonl"])
    check_cache_misses(node, "data.jsonl")

    node.query("desc file('data.jsonl')")
    check_cache_hits(node, "data.jsonl")

    node.query("insert into function file('data.jsonl') select * from numbers(100)")
    time.sleep(1)

    node.query("desc file('data.jsonl')")
    check_cache_invalidations(node, "data.jsonl")

    node.query("insert into function file('data1.jsonl') select * from numbers(100)")
    time.sleep(1)

    node.query("desc file('data1.jsonl')")
    check_cache(node, ["data.jsonl", "data1.jsonl"])
    check_cache_misses(node, "data1.jsonl")

    node.query("desc file('data1.jsonl')")
    check_cache_hits(node, "data1.jsonl")

    node.query("insert into function file('data2.jsonl') select * from numbers(100)")
    time.sleep(1)

    node.query("desc file('data2.jsonl')")
    check_cache(node, ["data1.jsonl", "data2.jsonl"])
    check_cache_misses(node, "data2.jsonl")
    check_cache_evictions(node, "data2.jsonl")

    node.query("desc file('data2.jsonl')")
    check_cache_hits(node, "data2.jsonl")

    node.query("desc file('data1.jsonl')")
    check_cache_hits(node, "data1.jsonl")

    node.query("desc file('data.jsonl')")
    check_cache(node, ["data.jsonl", "data1.jsonl"])
    check_cache_misses(node, "data.jsonl")
    check_cache_evictions(node, "data.jsonl")

    node.query("desc file('data2.jsonl')")
    check_cache(node, ["data.jsonl", "data2.jsonl"])
    check_cache_misses(node, "data2.jsonl")
    check_cache_evictions(node, "data2.jsonl")

    node.query("desc file('data2.jsonl')")
    check_cache_hits(node, "data2.jsonl")

    node.query("desc file('data.jsonl')")
    check_cache_hits(node, "data.jsonl")

    node.query("insert into function file('data3.jsonl') select * from numbers(100)")
    time.sleep(1)

    node.query("desc file('data*.jsonl')")
    check_cache(node, ["data.jsonl", "data1.jsonl"])
    check_cache_hits(node, "data*.jsonl")

    node.query("desc file('data.jsonl')")
    check_cache_hits(node, "data.jsonl")

    node.query("desc file('data1.jsonl')")
    check_cache_hits(node, "data1.jsonl")

    node.query("desc file('data2.jsonl')")
    check_cache_misses(node, "data2.jsonl")

    node.query("desc file('data3.jsonl')")
    check_cache_misses(node, "data3.jsonl")

    node.query("system drop schema cache for file")
    check_cache(node, [])

    node.query("desc file('data*.jsonl')")
    check_cache_misses(node, "data*.jsonl", 4)

    node.query("system drop schema cache")
    check_cache(node, [])

    node.query("desc file('data*.jsonl')")
    check_cache_misses(node, "data*.jsonl", 4)
