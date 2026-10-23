#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE}_delta_prewhere"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

$CLICKHOUSE_LOCAL --allow_delta_lake_writes=1 --allow_delta_lake_create_table=1 --query "
CREATE TABLE t_unpart (id Nullable(Int64), email Nullable(String), payload Nullable(String)) ENGINE = DeltaLakeLocal('${ROOT}/unpart', Parquet);
INSERT INTO t_unpart SELECT number, concat('u', toString(number), '@x'), repeat('p', 10) FROM numbers(1000);
INSERT INTO t_unpart SELECT number + 1000, concat('u', toString(number + 1000), '@x'), repeat('p', 10) FROM numbers(1000);

SELECT 'where', id FROM t_unpart WHERE email = 'u1500@x';
SELECT 'prewhere', id FROM t_unpart PREWHERE email = 'u1500@x';
SELECT 'prewhere and where', id, payload FROM t_unpart PREWHERE email LIKE 'u15%' WHERE id % 100 = 0 ORDER BY id;
SELECT 'prewhere output', id, length(email) AS l FROM t_unpart PREWHERE length(email) = 7 ORDER BY id LIMIT 3;
SELECT 'prewhere count', count() FROM t_unpart PREWHERE email LIKE 'u1%';
SELECT 'prewhere virtual', _file != '', id FROM t_unpart PREWHERE email = 'u7@x';
SELECT 'moved to prewhere', trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM t_unpart WHERE email = 'u1500@x') WHERE explain LIKE '%Prewhere filter column%';
"

SCHEMA='{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"email\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"part\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}'
mkdir -p "${ROOT}/part/_delta_log"
cat > "${ROOT}/part/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"${CLICKHOUSE_DATABASE}-part","format":{"provider":"parquet","options":{}},"schemaString":"${SCHEMA}","partitionColumns":["part"],"configuration":{},"createdTime":1700000000000}}
EOF

$CLICKHOUSE_LOCAL --allow_delta_lake_writes=1 --query "
INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/part') SELECT number AS id, concat('u', toString(number), '@x') AS email, toString(number % 3) AS part FROM numbers(300);
CREATE TABLE t_part (id Nullable(Int64), email Nullable(String), part Nullable(String)) ENGINE = DeltaLakeLocal('${ROOT}/part');

SELECT 'part where', id, part FROM t_part WHERE email = 'u150@x';
SELECT 'part prewhere', id, part FROM t_part PREWHERE email = 'u150@x';
SELECT 'part prewhere only partition column', part FROM t_part PREWHERE email = 'u151@x';
SELECT 'part both', id, part FROM t_part WHERE email LIKE 'u15%' AND part = '1' ORDER BY id;
SELECT 'part prewhere output', part, length(email) AS l FROM t_part PREWHERE length(email) = 4 ORDER BY id LIMIT 3;
SELECT 'part star', * FROM t_part PREWHERE email = 'u152@x';
SELECT 'part moved to prewhere', trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM t_part WHERE email = 'u150@x' AND part = '0') WHERE explain LIKE '%Prewhere filter column%';
SELECT id FROM t_part PREWHERE part = '1'; -- { serverError ILLEGAL_PREWHERE }
"

MAPPING="$CUR_DIR/data_delta_lake/struct_column_mapping"
$CLICKHOUSE_LOCAL --query "
SELECT 'mapping', c0 FROM deltaLakeLocal('${MAPPING}') WHERE c0 IS NOT NULL ORDER BY c0 LIMIT 2;
SELECT 'mapping moved to prewhere', count() FROM (EXPLAIN actions = 1 SELECT c0 FROM deltaLakeLocal('${MAPPING}') WHERE c0 IS NOT NULL) WHERE explain LIKE '%Prewhere%';
"
