#!/usr/bin/env bash
# Tags: atomic-database, no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Exactly-once incremental refreshable MV writing MergeTree -> Delta Lake. The advanced cursor is
# committed as a `domainMetadata` action in the same `_delta_log` commit as the data files, and the
# next refresh resumes from the cursor in the table rather than from the view's own state.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_delta"
TABLE_PATH_NO_FEATURE="${CLICKHOUSE_USER_FILES_UNIQUE}_delta_no_feature"
trap 'rm -rf "${TABLE_PATH}" "${TABLE_PATH_NO_FEATURE}" 2>/dev/null' EXIT
rm -rf "${TABLE_PATH}" "${TABLE_PATH_NO_FEATURE}"

$CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;

CREATE TABLE irmv_src (k Int64)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

-- The new Delta table gets the domainMetadata writer feature, which the refresh cursor needs.
CREATE TABLE irmv_tgt (k Int64) ENGINE = DeltaLakeLocal('${TABLE_PATH}', Parquet)
SETTINGS delta_lake_enable_domain_metadata = 1;

-- REFRESH EVERY 10 YEAR + EMPTY: no automatic refresh; every refresh below is triggered manually.
CREATE MATERIALIZED VIEW irmv_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO irmv_tgt EMPTY
    AS SELECT k FROM irmv_src;

-- Round 1: rows 0..4. The refresh appends them and commits the cursor with them.
INSERT INTO irmv_src SELECT number FROM numbers(5);
SYSTEM REFRESH VIEW irmv_mv;
SYSTEM WAIT VIEW irmv_mv;
SELECT 'round1', count(), uniqExact(k) FROM irmv_tgt;

-- A plain insert commits no cursor, and must not drop the one committed by round 1.
INSERT INTO irmv_tgt VALUES (100);
SELECT 'plain insert', count(), uniqExact(k) FROM irmv_tgt;

-- Re-creating the view discards every cursor the view itself holds (Atomic database: no Keeper znode),
-- so only the cursor in the Delta table can make the next round skip rows 0..4.
DROP TABLE irmv_mv;
CREATE MATERIALIZED VIEW irmv_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO irmv_tgt EMPTY
    AS SELECT k FROM irmv_src;

-- Round 2: rows 5..9. Only the new rows are appended: 11 rows, 11 distinct. A lost cursor gives 16.
INSERT INTO irmv_src SELECT number FROM numbers(5, 5);
SYSTEM REFRESH VIEW irmv_mv;
SYSTEM WAIT VIEW irmv_mv;
SELECT 'round2', count(), uniqExact(k) FROM irmv_tgt;

-- Round 3: nothing new; the refresh appends nothing.
SYSTEM REFRESH VIEW irmv_mv;
SYSTEM WAIT VIEW irmv_mv;
SELECT 'round3', count(), uniqExact(k) FROM irmv_tgt;
"

# The created table's protocol lists the feature.
$CLICKHOUSE_CLIENT --query "
SELECT 'writer features', JSONExtract(line, 'protocol', 'writerFeatures', 'Array(String)')
FROM file('${TABLE_PATH}/_delta_log/00000000000000000000.json', LineAsString)
WHERE JSONHas(line, 'protocol')
FORMAT TSV
"

# Commits 1, 3 and 4 are refreshes and carry the cursor; commit 2 is the plain insert.
$CLICKHOUSE_CLIENT --query "
SELECT
    toUInt64(splitByChar('.', _file)[1]) AS version,
    countIf(JSONExtractString(line, 'domainMetadata', 'domain') = 'clickhouse.refresh-cursor') AS cursors
FROM file('${TABLE_PATH}/_delta_log/*.json', LineAsString)
WHERE version > 0
GROUP BY version
ORDER BY version
FORMAT TSV
"

# A table created without delta_lake_enable_domain_metadata lacks the feature, so the cursor cannot be
# committed with the data: the refresh fails instead of appending rows that the next round would append again.
$CLICKHOUSE_CLIENT --query "
SET allow_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;
CREATE TABLE irmv_tgt_no_feature (k Int64) ENGINE = DeltaLakeLocal('${TABLE_PATH_NO_FEATURE}', Parquet);
CREATE MATERIALIZED VIEW irmv_mv_no_feature
    REFRESH EVERY 10 YEAR SETTINGS refresh_retries = 0 APPEND INCREMENTAL
    TO irmv_tgt_no_feature EMPTY
    AS SELECT k FROM irmv_src;
SYSTEM REFRESH VIEW irmv_mv_no_feature;
"
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT VIEW irmv_mv_no_feature" 2>&1 | grep -q "'domainMetadata' writer feature" && echo "no domainMetadata feature: refresh failed"

$CLICKHOUSE_CLIENT --query "
SELECT 'no domainMetadata feature', count() FROM irmv_tgt_no_feature;

DROP TABLE irmv_mv_no_feature;
DROP TABLE irmv_tgt_no_feature;
DROP TABLE irmv_mv;
DROP TABLE irmv_tgt;
DROP TABLE irmv_src;
"
