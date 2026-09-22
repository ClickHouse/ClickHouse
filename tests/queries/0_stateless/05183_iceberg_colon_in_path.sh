#!/usr/bin/env bash
# Tags: no-fasttest

# A path in Iceberg metadata need not be a URI, and an object key may legitimately contain a colon --
# an unescaped partition value, say -- so text before a colon names a scheme only when it is shaped
# like one. The table writes scheme-less absolute paths (`write_full_path_in_iceberg_metadata = 0`,
# the default), so its manifest entries read `<work dir>/t0:/data/<uuid>.parquet`.
#
# Under `clickhouse local`, whose path prefix is the root, because a server resolves the engine
# argument against `user_files_path`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/iceberg_colon_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
# The trailing ':' is the point of the test: it makes the table directory look like a URI scheme.
TABLE_DIR="${WORK_DIR}/t0:"
mkdir -p "${TABLE_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --multiquery -q "
CREATE TABLE t0 (c0 Int) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO t0 VALUES (42);
SELECT c0 FROM t0;
"
