#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

# A legacy `toTime` hidden in a SQL UDF body must be inlined and canonicalized before the standalone
# UPDATE / DELETE query text is enqueued, so a replaying host without this session's settings (the
# oldest DDL entry format carries none) resolves the same expression. The UDF name includes the test
# database because SQL UDFs are server-global and this test runs concurrently with itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UDF="totime_05051_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --use_legacy_to_time 1 --distributed_ddl_entry_format_version 1 \
    --distributed_ddl_output_mode none --enable_lightweight_update 1 \
    --lightweight_delete_mode lightweight_update_force -q "
CREATE FUNCTION ${UDF} AS x -> toUInt32(toTime(x));

CREATE TABLE ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu (c0 DateTime('UTC'), v UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu VALUES ('2020-01-02 03:04:05', 0);

SELECT 'session', ${UDF}(c0) FROM ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu;

UPDATE ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu ON CLUSTER test_shard_localhost
    SET v = ${UDF}(c0) WHERE 1;
SELECT 'updated', v FROM ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu;

DELETE FROM ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu ON CLUSTER test_shard_localhost
    WHERE ${UDF}(c0) = 97445;
SELECT 'after_delete', count() FROM ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu;

DROP TABLE ${CLICKHOUSE_DATABASE}.t_totime_udf_lwu;
DROP FUNCTION ${UDF};
"
