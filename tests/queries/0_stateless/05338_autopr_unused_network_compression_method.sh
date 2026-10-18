#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `automatic_parallel_replicas_mode` prices the parallel-replicas plan with the wire codec, which
# `network_compression_method` resolves to. The codec must be resolved only for a query that reads from a
# `MergeTree` table and collects the statistics: an uncompressed HTTP query without such a read never uses
# the setting, so an invalid value of it must not make the query fail.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&automatic_parallel_replicas_mode=1&network_compression_method=T64" -d "SELECT 1"
