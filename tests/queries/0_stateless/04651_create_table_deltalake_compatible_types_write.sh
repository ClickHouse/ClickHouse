#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan
#
# When CREATE stores a column as a compatible-but-different Delta type, the table must adopt the persisted
# type (a `UInt8` becomes `Int16`, a `FixedString` becomes `String`), so the data files a later INSERT writes
# stay consistent with the Delta metadata and a fresh re-attach reads the same values back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_compatible_types_write"

rm -rf "$TABLE_PATH"

# Create with compatible types; the table must adopt the persisted (Delta) types.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_norm (n UInt8, s FixedString(3)) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_dl_norm' ORDER BY name;
INSERT INTO t_dl_norm VALUES (200, 'abc');
SELECT n, s FROM t_dl_norm;
"

# A fresh re-attach reads the same values back, confirming the metadata matches the physical data files.
$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
CREATE TABLE t_dl_norm_reattach ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_dl_norm_reattach' ORDER BY name;
SELECT n, s FROM t_dl_norm_reattach;
DROP TABLE t_dl_norm_reattach;
DROP TABLE t_dl_norm;
"

rm -rf "$TABLE_PATH"
