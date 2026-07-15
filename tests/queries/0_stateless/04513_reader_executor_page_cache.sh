#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the userspace page cache is configured via a per-run config file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stateless server runs with the page cache disabled (page_cache_max_size = 0), so exercise the
# executor's page-cache chain in clickhouse-local with the cache enabled, on a local MergeTree.
CONFIG_FILE="${CLICKHOUSE_TMP}/04513_page_cache_config.yaml"
echo "page_cache_max_size: 134217728" > "${CONFIG_FILE}"

${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" \
    --use_reader_executor 1 \
    --use_page_cache_for_local_disks 1 \
    --local_filesystem_read_method pread \
    --query "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t SELECT number FROM numbers(200000);

-- Cold read populates the page cache through the executor; warm read serves from it.
SELECT count(), sum(x) FROM t;
SELECT count(), sum(x) FROM t;

-- ReaderExecutorCacheGetRequests is emitted only by the executor's cache chain, so a positive
-- global count proves the executor engaged AND consulted the page cache.
SELECT sum(value) > 0 FROM system.events WHERE event = 'ReaderExecutorCacheGetRequests';
"
