#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} --query "SELECT * FROM (SELECT 'ALTER TABLE src' AS query, 123 AS query_duration_ms) INTO OUTFILE '${CLICKHOUSE_TMP}/query_log.tsv.zst' FORMAT TSVWithNames"
${CLICKHOUSE_LOCAL} --query "
SELECT *
FROM '${CLICKHOUSE_TMP}/query_log.tsv.zst'
WHERE query_duration_ms = (
    SELECT max(query_duration_ms)
    FROM '${CLICKHOUSE_TMP}/query_log.tsv.zst'
    WHERE query LIKE 'ALTER TABLE src%'
)
LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) ENGINE = Memory();
INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}.values', 'Values') SELECT * FROM (SELECT 1 FROM remote('${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_TCP}', currentDatabase(), 't0') x) x;
DROP TABLE t0;
"
