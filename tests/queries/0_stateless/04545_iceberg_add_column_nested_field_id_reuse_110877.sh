#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/110877
#
# createEmptyMetadataFile initialized "last-column-id" to the number of
# TOP-LEVEL columns, ignoring the nested field ids that tuple/array/map children
# consume. For `(id Int64, t Tuple(a, b))` the field ids are id=1, t=2, t.a=3,
# t.b=4, but "last-column-id" was written as 2. A subsequent ADD COLUMN then
# reused id 3 (already held by t.a), publishing metadata with a duplicate field
# id: the ALTER succeeded but the next SELECT failed with
# `Code: 743 ... Duplicate field id 3 (ICEBERG_SPECIFICATION_VIOLATION)`.
#
# The fix records the max assigned field id (counting nested children) as
# "last-column-id" on initial table creation, and in generateAddColumnMetadata
# assigns nested children of a newly added complex column their own ids so every
# newly added field id is globally unique.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique table name and on-disk path so the test is parallel-safe.
TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_DIR="${USER_FILES_PATH}/${TABLE}"
rm -rf "${TABLE_DIR}"
trap 'rm -rf "${TABLE_DIR}"; ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"' EXIT

# last-column-id of the latest on-disk metadata version.
last_column_id() {
    local latest
    latest=$(find "${TABLE_DIR}/metadata" -name 'v*.metadata.json' | sort -V | tail -1)
    python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['last-column-id'])" "${latest}"
}

# The initial schema has a nested tuple, so nested children consume field ids
# past the top-level column count. A SELECT after each ALTER re-reads the
# published metadata, so a duplicate field id surfaces immediately. Adding the
# complex nested column `s` then reading exercises the nested half of the fix:
# if generateAddColumnMetadata regressed to storing `new_field_id` instead of
# the max nested child id, a later ADD COLUMN would reuse a child id of `s`.
# DROP COLUMN d then ADD COLUMN e checks that dropped ids are never reused: `d`
# (id 9) is dropped, `e` must still take a fresh id 10, so last-column-id stays
# monotonic at 10.
${CLICKHOUSE_CLIENT} \
    --allow_insert_into_iceberg=1 \
    --enable_nullable_tuple_type=1 \
    --multiquery -q "
CREATE TABLE ${TABLE} (id Int64, t Tuple(a Int64, b Int64)) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO ${TABLE} VALUES (1, (10, 20));
ALTER TABLE ${TABLE} ADD COLUMN c Nullable(Int64);
SELECT id, t FROM ${TABLE} ORDER BY id;
ALTER TABLE ${TABLE} ADD COLUMN s Nullable(Tuple(x Int64, y Int64));
SELECT id, t FROM ${TABLE} ORDER BY id;
ALTER TABLE ${TABLE} ADD COLUMN d Nullable(Int64);
SELECT id, t FROM ${TABLE} ORDER BY id;
ALTER TABLE ${TABLE} DROP COLUMN d;
ALTER TABLE ${TABLE} ADD COLUMN e Nullable(Int64);
SELECT id, t FROM ${TABLE} ORDER BY id;
INSERT INTO ${TABLE} (id, t) VALUES (2, (30, 40));
SELECT id, t FROM ${TABLE} ORDER BY id;
"
# ids: id=1 t=2 (t.a=3 t.b=4) c=5 s=6 (s.x=7 s.y=8) d=9; DROP d keeps 9; e=10.
echo "last-column-id=$(last_column_id)"
