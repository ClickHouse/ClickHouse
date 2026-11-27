#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs the Avro and Parquet libraries, which the fast test does not build.

# `use_page_cache_for_object_storage` promises to "use userspace page cache when reading from object
# storage table functions ... and table engines" (the setting's description in src/Core/Settings.cpp).
# An Iceberg table over local object storage is such a read, and the Parquet reader fetches column
# chunks with positional (random) reads rather than sequentially, so the cached buffer has to serve
# those out of the page cache: populate the blocks a read lands in on a cold miss, fetch a run of
# neighbouring missing blocks in one go, and then answer the next query from the blocks already held.
# The test reads the same table twice through that path, checks that all rows come back whole on
# the cold pass as well as on the warm one, and checks per query in `system.query_log` that the cold
# pass misses the cache while the warm pass is served from it without a single miss.
#
# The CI server configures no page cache (`page_cache_max_size` is 0 by default), so everything runs
# in a private clickhouse-local that has one, as in 04039_page_cache_in_local.sh and
# 05293_packed_archive_read_through_page_cache.sh.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `IcebergLocal` refuses a path outside the working directory of its `clickhouse local`, and
# ${CLICKHOUSE_TMP} is not necessarily inside it, so everything lives in a uniquely named
# subdirectory of the working directory (which is what ${CLICKHOUSE_TMP} falls back to anyway).
work_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache"
rm -rf "./${work_dir}"
mkdir -p "./${work_dir}"

> "./${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
query_log:
    database: system
    table: query_log
"

# Pinned because tests/clickhouse-test randomizes all but the third and the last of these:
#  - use_page_cache_for_object_storage: off, the read never reaches the page cache at all;
#  - remote_filesystem_read_method=read: `threadpool` puts an asynchronous prefetch in front of the
#    cache, and that answers a small object from its own prefetch buffer instead;
#  - page_cache_block_size=65536, small against the ~300 KiB data files, so a read can miss several
#    adjacent blocks at once and the coalescing path runs too, not only the single-block one;
#  - page_cache_max_coalesced_bytes=1048576, large enough to keep such a run in one fetch;
#  - page_cache_inject_eviction and read_from_page_cache_if_exists_otherwise_bypass_cache off, so the
#    second query really reads back what the first one stored.
page_cache=(
    --config-file "./${work_dir}/config.yaml"
    --use_page_cache_for_object_storage 1
    --remote_filesystem_read_method read
    --page_cache_block_size 65536
    --page_cache_max_coalesced_bytes 1048576
    --page_cache_inject_eviction 0
    --read_from_page_cache_if_exists_otherwise_bypass_cache 0
)

# The writing query never reads the data files back, so it cannot warm the cache for them; that the
# cold read below really starts from nothing is asserted per query and not assumed.
# The key is written as Int64: the widest Iceberg integer type is the signed 64-bit long, so Iceberg
# rejects the UInt64 that `number` has.
${CLICKHOUSE_LOCAL} --path "./${work_dir}/db" "${page_cache[@]}" --allow_insert_into_iceberg 1 --log_queries 1 --query "
    CREATE TABLE ice ENGINE = IcebergLocal('${work_dir}/icedata/')
    AS SELECT toInt64(number) AS k, repeat('x', 200) AS v FROM numbers(200000);
    -- Cold cache: every block the Parquet reader asks for is a miss and has to be populated.
    SELECT count(), sum(k), uniqExact(v) FROM icebergLocal('${work_dir}/icedata/', 'Parquet') SETTINGS log_comment = 'cold';
    -- Warm cache: the same blocks are already held, and the rows must still be the same.
    SELECT count(), sum(k) FROM icebergLocal('${work_dir}/icedata/', 'Parquet') SETTINGS log_comment = 'warm';
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['PageCacheMisses'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'cold';
    SELECT ProfileEvents['PageCacheMisses'], ProfileEvents['PageCacheHits'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'warm';"

rm -rf "./${work_dir}"
