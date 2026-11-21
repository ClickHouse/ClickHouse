#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# no-replicated-database: the test creates replicated tables with explicit ZooKeeper paths.
# When the table definition is the same as in the backup, RESTORE keeps the level of a merged part, so FINAL
# can still read it without collapsing. When it is not checked (allow_different_table_def), the level is reset.
# GraphiteMergeTree is always reset: its rollup rules come from the server config, which the backup does not carry.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup_rmt="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_rmt')"
backup_rep="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_rep')"
backup_plain="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_plain')"
backup_graphite="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_graphite')"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE rmt (a UInt32, b UInt32) ENGINE = ReplacingMergeTree ORDER BY a;
INSERT INTO rmt VALUES (1, 10), (2, 30), (3, 50);
INSERT INTO rmt VALUES (1, 20), (2, 40), (3, 60);
OPTIMIZE TABLE rmt FINAL;
BACKUP TABLE rmt TO ${backup_rmt} FORMAT Null;
DROP TABLE rmt SYNC;
RESTORE TABLE rmt FROM ${backup_rmt} FORMAT Null;
SELECT 'rmt_level_gt0', max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'rmt' AND active;
SELECT 'rmt_rows_final', count() FROM rmt FINAL;
"

# SYNC, so that the RESTORE below can create the table at the same ZooKeeper path.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE rep (a UInt32, b UInt32) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/rep', 'r1') ORDER BY a;
INSERT INTO rep VALUES (1, 10), (2, 30), (3, 50);
INSERT INTO rep VALUES (1, 20), (2, 40), (3, 60);
OPTIMIZE TABLE rep FINAL;
BACKUP TABLE rep TO ${backup_rep} FORMAT Null;
DROP TABLE rep SYNC;
RESTORE TABLE rep FROM ${backup_rep} FORMAT Null;
SELECT 'rep_level_gt0', max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'rep' AND active;
SELECT 'rep_rows_final', count() FROM rep FINAL;
"

# A merged part of a plain MergeTree has level > 0 while still holding duplicate ORDER BY keys.
${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE plain (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO plain VALUES (1, 10), (2, 30), (3, 50);
INSERT INTO plain VALUES (1, 20), (2, 40), (3, 60);
OPTIMIZE TABLE plain FINAL;
BACKUP TABLE plain TO ${backup_plain} FORMAT Null;
CREATE TABLE rep_dst (a UInt32, b UInt32) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{database}/rep_dst', 'r1') ORDER BY a;
SYSTEM STOP MERGES rep_dst;
RESTORE TABLE plain AS rep_dst FROM ${backup_plain} SETTINGS allow_different_table_def = 1 FORMAT Null;
SELECT 'rep_dst_level', level FROM system.parts
    WHERE database = currentDatabase() AND table = 'rep_dst' AND active;
SELECT 'rep_dst_rows_final', count() FROM rep_dst FINAL;
"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE graphite (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
    ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
INSERT INTO graphite VALUES ('sum_1', toDateTime('2026-01-01 00:00:00', 'UTC'), 1, 1);
INSERT INTO graphite VALUES ('sum_1', toDateTime('2026-01-01 00:00:30', 'UTC'), 2, 1);
OPTIMIZE TABLE graphite FINAL;
SELECT 'graphite_src_level_gt0', max(level) > 0 FROM system.parts
    WHERE database = currentDatabase() AND table = 'graphite' AND active;
BACKUP TABLE graphite TO ${backup_graphite} FORMAT Null;
DROP TABLE graphite SYNC;
RESTORE TABLE graphite FROM ${backup_graphite} FORMAT Null;
SELECT 'graphite_level', max(level) FROM system.parts
    WHERE database = currentDatabase() AND table = 'graphite' AND active;
"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE graphite SYNC;
DROP TABLE rep_dst SYNC;
DROP TABLE plain SYNC;
DROP TABLE rep SYNC;
DROP TABLE rmt SYNC;
"
