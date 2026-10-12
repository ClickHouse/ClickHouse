#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/packed_finalization_fds.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Delaying packed parts must not retain one local archive descriptor per partition.
(
    ulimit -n 256
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}" --multiquery --query "
        CREATE TABLE t (p UInt64, v UInt64)
        ENGINE = MergeTree PARTITION BY p ORDER BY v
        SETTINGS min_bytes_for_full_part_storage = '32M', min_bytes_for_wide_part = '32M';

        INSERT INTO t SELECT number % 400, number FROM numbers(4000)
        SETTINGS max_threads = 1, max_insert_threads = 1,
            max_partitions_per_insert_block = 400,
            max_insert_delayed_streams_for_parallel_write = 1000000;

        SELECT count(), sum(v) FROM t SETTINGS max_threads = 1;
    "
)
