#!/usr/bin/env bash
# A merged part of a plain MergeTree has level > 0 while still holding duplicate ORDER BY keys.
# Restoring it into a ReplacingMergeTree is allowed by allow_different_table_def, and FINAL treats
# a lone level > 0 part as already collapsed, so the level must not be carried over.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE src (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO src VALUES (1, 10), (2, 30), (3, 50);
INSERT INTO src VALUES (1, 20), (2, 40), (3, 60);
OPTIMIZE TABLE src FINAL;
SELECT 'src_level_gt0', max(level) > 0 FROM system.parts
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

# The level of a part restored into an engine that merges rows without collapsing them is kept.
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
DROP TABLE dst_plain SYNC;
DROP TABLE dst_replacing SYNC;
DROP TABLE src SYNC;
"
