#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# A DeltaLake CREATE keeps the user's declared column types (a `UInt8` column stays `UInt8`), while a later
# INSERT casts the data to the compatible Delta type stored in the log (`UInt8` -> `short`, `FixedString` ->
# `string`), so the data files stay consistent with the Delta metadata and a fresh re-attach reads the same
# values back. A columnless re-attach uses the Delta log's own types.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_compatible_types_write"

rm -rf "$TABLE_PATH"

# Create with compatible types; the table keeps the declared types.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_experimental_delta_lake_create_table = 1;
CREATE TABLE t_dl_norm (n UInt8, s FixedString(3)) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_dl_norm' ORDER BY name;
INSERT INTO t_dl_norm VALUES (200, 'abc');
SELECT n, s FROM t_dl_norm;
"

# A fresh re-attach reads the same values back, confirming the metadata matches the physical data files.
# Re-attaching with the original compatible column types must also succeed (the CREATE is repeatable, since
# the declared types map to the same Delta types the table was stored with).
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_experimental_delta_lake_create_table = 1;
CREATE TABLE t_dl_norm_reattach ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_dl_norm_reattach' ORDER BY name;
SELECT n, s FROM t_dl_norm_reattach;
CREATE TABLE t_dl_norm_reattach_explicit (n UInt8, s FixedString(3)) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT n, s FROM t_dl_norm_reattach_explicit;
DROP TABLE t_dl_norm_reattach_explicit;
DROP TABLE t_dl_norm_reattach;
DROP TABLE t_dl_norm;
"

rm -rf "$TABLE_PATH"
