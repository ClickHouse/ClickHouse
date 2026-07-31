#!/usr/bin/env bash
# Tags: no-fasttest

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104295
#
# When `iceberg_metadata_staleness_ms` is non-zero, the metadata files cache
# may serve a pre-`ALTER` schema view to readers and to the upstream `INSERT`
# pipeline, while `IcebergStorageSink` always reads fresh metadata. The two
# views disagree on the column count, and `DataFileStatistics::getColumnSizes`
# accesses `field_ids` (sized by the post-`ALTER` schema) using an index from
# `column_sizes` (sized by the pre-`ALTER` chunk), which causes an
# out-of-bounds vector access and aborts the process.
#
# `Iceberg::alter` writes a new metadata file but did not invalidate the cache,
# while `IcebergStorageSink::initializeMetadata` did invalidate it. The fix is
# to invalidate the cache in `Iceberg::alter` after writing the new metadata
# file, mirroring the existing invalidation in `IcebergStorageSink`.
#
# The reproducer runs in `clickhouse local` (rather than against the test
# server) because the underlying bug aborts the process with `SIGABRT`. With
# `clickhouse-client` the abort lands inside the long-running test server,
# which causes the test runner to be terminated by the hung-check before it
# can record a `FAIL` for this test, so the `Bugfix validation` framework
# cannot invert the result to `OK`. Running the reproducer in
# `clickhouse local` contains the abort to a single short-lived subprocess:
# the test runner observes a non-zero exit and an empty stdout, both of which
# are diff'd against the `.reference` file and reported as a normal `FAIL`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Isolated work dir for the `IcebergLocal` table. Passed to `clickhouse local`
# as `--user_files_path` so the `IcebergLocal` access check accepts the path.
WORK_DIR="${CLICKHOUSE_TMP}/iceberg_alter_drop_column_104295_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

TABLE_DIR="${WORK_DIR}/t0"
mkdir -p "${TABLE_DIR}"

${CLICKHOUSE_LOCAL} \
    --allow_insert_into_iceberg=1 \
    --iceberg_metadata_staleness_ms=60000 \
    --multiquery -q "
CREATE TABLE t0 (c0 Int, c1 Int) ENGINE = IcebergLocal('${TABLE_DIR}/');
INSERT INTO t0 (c1, c0) VALUES (1, 1);
ALTER TABLE t0 DROP COLUMN c0;
INSERT INTO t0 (c1) SELECT 2;
SELECT c1 FROM t0 ORDER BY c1;
" -- --user_files_path="${WORK_DIR}"
