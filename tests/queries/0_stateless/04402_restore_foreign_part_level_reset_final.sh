#!/usr/bin/env bash
# Restoring a part of one table into another is allowed by allow_different_table_def, and neither
# of the provenance numbers in its name means anything in the destination.
# A merged part of a plain MergeTree has level > 0 while still holding duplicate ORDER BY keys, and
# FINAL treats a lone level > 0 part as already collapsed, so the level must not be carried over.
# Mutation versions are numbered per table, so a carried-over one must not be able to place the part
# ahead of mutations the destination issues later.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE src (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src VALUES (1, 10), (2, 30), (3, 50);
INSERT INTO src VALUES (1, 20), (2, 40), (3, 60);
OPTIMIZE TABLE src FINAL;
-- The predicate matches no row, so this only advances the source's own mutation counter.
SET mutations_sync = 1;
ALTER TABLE src DELETE WHERE b = 999;
SELECT 'src_level_gt0', max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'src' AND active;
SELECT 'src_data_version_gt0', max(data_version) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'src' AND active;
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE src TO ${backup}" | grep -o BACKUP_CREATED

# A background merge would collapse the lone restored part on its own, and the assertions below
# would then hold whatever level it was restored with.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE dst_replacing (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
SYSTEM STOP MERGES dst_replacing;
"
${CLICKHOUSE_CLIENT} --query "
RESTORE TABLE src AS dst_replacing FROM ${backup} SETTINGS allow_different_table_def = 1
" | grep -o RESTORED

${CLICKHOUSE_CLIENT} -m --query "
SELECT 'replacing_level', level FROM system.parts
    WHERE database = currentDatabase() AND table = 'dst_replacing' AND active;
SELECT 'replacing_rows', count() FROM dst_replacing;
SELECT 'replacing_rows_final', count() FROM dst_replacing FINAL;
"

# Merges are stopped, so the mutation below can only be observed through the on-fly path, and
# waiting for it to materialize would never return.
${CLICKHOUSE_CLIENT} -m --query "
SET apply_mutations_on_fly = 1, mutations_sync = 0;
ALTER TABLE dst_replacing DELETE WHERE a = 1;
SELECT 'replacing_rows_after_own_delete', count() FROM dst_replacing;
"

# The level of a part restored into an engine that merges rows without collapsing them is kept,
# while the mutation version is dropped for every engine.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE dst_plain (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
SYSTEM STOP MERGES dst_plain;
"
${CLICKHOUSE_CLIENT} --query "
RESTORE TABLE src AS dst_plain FROM ${backup} SETTINGS allow_different_table_def = 1
" | grep -o RESTORED

${CLICKHOUSE_CLIENT} -m --query "
SELECT 'plain_level_gt0', max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'dst_plain' AND active;
"

${CLICKHOUSE_CLIENT} -m --query "
SET apply_mutations_on_fly = 1, mutations_sync = 0;
ALTER TABLE dst_plain DELETE WHERE a = 1;
SELECT 'plain_rows_after_own_delete', count() FROM dst_plain;
"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE dst_plain SYNC;
DROP TABLE dst_replacing SYNC;
DROP TABLE src SYNC;
"
