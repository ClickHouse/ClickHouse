#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH_UNPART="${CLICKHOUSE_USER_FILES_UNIQUE}_unpart"
TABLE_PATH_PART="${CLICKHOUSE_USER_FILES_UNIQUE}_part"

rm -rf "$TABLE_PATH_UNPART" "$TABLE_PATH_PART"

$CLICKHOUSE_CLIENT --query "
SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;

DROP TABLE IF EXISTS t_dl_unpart;
CREATE TABLE t_dl_unpart (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH_UNPART}', Parquet);

INSERT INTO t_dl_unpart SELECT number, toString(number) FROM numbers(5);

SELECT id, name FROM t_dl_unpart ORDER BY id;
SELECT count() FROM t_dl_unpart;

DROP TABLE IF EXISTS t_dl_part;
CREATE TABLE t_dl_part (id Int32, name String, country String)
    ENGINE = DeltaLakeLocal('${TABLE_PATH_PART}', Parquet)
    PARTITION BY country;

-- The kernel's FFI does not yet expose partitioned_write_context, so INSERT into a
-- partitioned Delta table cannot be tested here. Verify the empty table is readable
-- and reports the configured partition column instead.
SELECT count() FROM t_dl_part;
SELECT name FROM system.columns WHERE table = 't_dl_part' AND database = currentDatabase() ORDER BY name;

-- IF NOT EXISTS path on an already-created table is a no-op.
CREATE TABLE IF NOT EXISTS t_dl_unpart (id Int32, name String) ENGINE = DeltaLakeLocal('${TABLE_PATH_UNPART}', Parquet);
SELECT count() FROM t_dl_unpart;

DROP TABLE t_dl_unpart;
DROP TABLE t_dl_part;
"

rm -rf "$TABLE_PATH_UNPART" "$TABLE_PATH_PART"
