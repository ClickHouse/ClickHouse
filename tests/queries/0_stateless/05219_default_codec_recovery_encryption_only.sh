#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree, no-object-storage
# no-fasttest: AES_128_GCM_SIV depends on OpenSSL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_codec_recovery"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_codec_recovery (n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, default_compression_codec = 'AES_128_GCM_SIV'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_codec_recovery SELECT if(number % 3 = 0, NULL, number) FROM numbers(1000)"
${CLICKHOUSE_CLIENT} -q "SELECT default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = 't_codec_recovery' AND active"

part_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_codec_recovery' AND active")
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_codec_recovery SYNC"
rm "${part_path}default_compression_codec.txt"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_codec_recovery"

${CLICKHOUSE_CLIENT} -q "SELECT default_compression_codec FROM system.parts WHERE database = currentDatabase() AND table = 't_codec_recovery' AND active"
${CLICKHOUSE_CLIENT} -q "SELECT count(), countIf(n IS NULL) FROM t_codec_recovery"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_codec_recovery"
