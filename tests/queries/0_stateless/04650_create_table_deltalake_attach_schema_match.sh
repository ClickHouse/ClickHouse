#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# CREATE TABLE over an existing `_delta_log`:
#  - columnless        -> attaches with the table's own schema
#  - explicit columns  -> attaches with the declared columns (adapted to the data on read)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_attach_schema_match"
INITIAL_LOG="${TABLE_PATH}/_delta_log/00000000000000000000.json"

rm -rf "$TABLE_PATH"

# (1) Fresh create with an explicit schema writes commit 0.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_fresh (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
"
[ -f "$INITIAL_LOG" ] && echo "fresh: created" || echo "fresh: fail: no initial commit"

# (2) Attaching with a matching explicit schema succeeds.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_match (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT count() FROM t_dl_match;
"
echo "match: attached"

# (3) Attaching with a differing explicit type is accepted; the declared columns are kept and adapted to
#     the data on read (flexible attach).
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_differing (id Int64, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT count() FROM t_dl_differing;
"
echo "differing: attached"

# (4) A columnless attach uses the table's own schema.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_columnless ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT count() FROM t_dl_columnless;
"
echo "columnless: attached"

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_dl_fresh;
DROP TABLE IF EXISTS t_dl_match;
DROP TABLE IF EXISTS t_dl_differing;
DROP TABLE IF EXISTS t_dl_columnless;
"
rm -rf "$TABLE_PATH"
