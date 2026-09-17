"""The maximum entry size of the query cache (server setting `query_cache.max_entry_size_in_bytes`) also applies to the query cache on
disk, in particular when writes to the in-memory query cache are disabled and the on-disk cache is the only backend.

Each backend measures an entry the way it stores it: the in-memory cache by the weight of its columns, the on-disk cache by the size of
the serialized entry (header, access metadata, `Native` framing, compression). The two differ in both directions, so the limit must be
enforced against the serialized size on disk, not against the in-memory weight.

This needs an integration test because the limit is a server setting, and `clickhouse-local` (used by the stateless tests of the query
cache on disk) hardcodes it to 0.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/query_cache_on_disk.xml"],
    stay_alive=True,
)

SETTINGS = (
    "use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results', "
    "enable_writes_to_query_cache = 0"
)

# The same, but writing to both backends, so that the in-memory weight of an entry can be read from `system.query_cache`.
SETTINGS_BOTH_BACKENDS = (
    "use_query_cache = 1, query_cache_on_disk_cache_name = 'cache_for_query_results'"
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/query_cache_on_disk.xml"

# The limit currently written in the config, see `set_max_entry_size_in_bytes`.
current_max_entry_size_in_bytes = 1


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_event(name):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{name}'"
        ).strip()
        or 0
    )


def test_oversized_result_is_not_stored_on_disk(started_cluster):
    # The limit is 1 byte here, so even this tiny result (a `ColumnConst`, which is stored in its compact representation) exceeds it.
    query = f"SELECT 1 FROM numbers(1000) SETTINGS {SETTINGS}"

    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    node.query(query)
    assert get_event("QueryCacheOnDiskWrittenBytes") == written_before

    # Nothing was stored, so a repeated run cannot be served from disk.
    hits_before = get_event("QueryCacheOnDiskHits")
    node.query(query)
    assert get_event("QueryCacheOnDiskHits") == hits_before


def set_max_entry_size_in_bytes(limit):
    global current_max_entry_size_in_bytes
    node.replace_in_config(
        CONFIG_PATH,
        f"<max_entry_size_in_bytes>{current_max_entry_size_in_bytes}</max_entry_size_in_bytes>",
        f"<max_entry_size_in_bytes>{limit}</max_entry_size_in_bytes>",
    )
    current_max_entry_size_in_bytes = limit
    node.restart_clickhouse()


def measure_entry_sizes(query, tag, query_settings=""):
    """Runs the query with both backends enabled and returns the in-memory weight and the serialized on-disk size of its entry."""
    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    node.query(
        f"{query} SETTINGS {SETTINGS_BOTH_BACKENDS}, query_cache_tag = '{tag}'{query_settings} FORMAT Null"
    )
    serialized_size = get_event("QueryCacheOnDiskWrittenBytes") - written_before
    in_memory_weight = int(
        node.query(
            f"SELECT result_size FROM system.query_cache WHERE tag = '{tag}'"
        ).strip()
    )
    assert serialized_size > 0
    return in_memory_weight, serialized_size


def is_stored_on_disk(query, tag, query_settings=""):
    """Runs a query which is written to disk only and tells whether an entry was stored (and is served afterwards)."""
    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    hits_before = get_event("QueryCacheOnDiskHits")
    for _ in range(2):
        node.query(
            f"{query} SETTINGS {SETTINGS}, query_cache_tag = '{tag}'{query_settings} FORMAT Null"
        )
    written = get_event("QueryCacheOnDiskWrittenBytes") > written_before
    hit = get_event("QueryCacheOnDiskHits") == hits_before + 1
    assert written == hit
    return written


def test_result_within_the_limit_is_stored_on_disk(started_cluster):
    set_max_entry_size_in_bytes(1073741824)

    query = f"SELECT number FROM numbers(1000) SETTINGS {SETTINGS}"

    written_before = get_event("QueryCacheOnDiskWrittenBytes")
    node.query(query)
    assert get_event("QueryCacheOnDiskWrittenBytes") > written_before

    hits_before = get_event("QueryCacheOnDiskHits")
    node.query(query)
    assert get_event("QueryCacheOnDiskHits") == hits_before + 1


def test_limit_applies_to_the_serialized_size(started_cluster):
    # A single-row `Const` result: its in-memory weight is one padded allocation, while the serialized entry additionally carries
    # the fixed header, the access metadata and the compression framing, so the serialized entry is the larger one. The sizes are
    # measured at runtime instead of being hardcoded, since both depend on allocator and codec details.
    query = "SELECT 1"
    in_memory_weight, serialized_size = measure_entry_sizes(query, "const_measure")
    assert in_memory_weight < serialized_size

    # A limit which the in-memory weight satisfies but the serialized entry does not: the entry must not be stored on disk.
    set_max_entry_size_in_bytes(serialized_size - 1)
    assert not is_stored_on_disk(query, "const_between")

    # Sanity check of the shape: one byte more and the entry fits.
    set_max_entry_size_in_bytes(serialized_size)
    assert is_stored_on_disk(query, "const_fits")


def test_limit_does_not_apply_to_the_in_memory_weight(started_cluster):
    # The opposite direction: many one-row chunks are padded allocations in memory, but compress to a small entry on disk. The limit
    # is about the bytes stored on disk, so such a result must be stored even though its in-memory weight exceeds the limit.
    set_max_entry_size_in_bytes(
        1073741824
    )  # both backends must accept the entry to measure it

    query = "SELECT number FROM numbers(100)"
    query_settings = ", max_block_size = 1, query_cache_squash_partial_results = 0"
    in_memory_weight, serialized_size = measure_entry_sizes(
        query, "chunks_measure", query_settings
    )
    assert serialized_size < in_memory_weight

    set_max_entry_size_in_bytes(in_memory_weight - 1)
    assert is_stored_on_disk(query, "chunks_between", query_settings)
