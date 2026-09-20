#!/usr/bin/env bash
# A predicate on `engine` is answered per table, which is a block and an expression run each time, so the answer
# is remembered for the engine. It holds for one database only: the predicate may read `database` as well, and
# the session's temporary tables report an empty one. A memory of it that outlived its database silently dropped
# a temporary table's rows when a catalog database happened to hold a table of the same engine - and, the other
# way round, would have reported a temporary table the predicate excludes.
#
# A shell test because the temporary table and the queries have to share one session.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS catalog_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS catalog_memory"
$CLICKHOUSE_CLIENT -q "CREATE TABLE catalog_log (a UInt8) ENGINE = Log"
$CLICKHOUSE_CLIENT -q "CREATE TABLE catalog_memory (a UInt8) ENGINE = Memory"

$CLICKHOUSE_CLIENT -mn -q "
CREATE TEMPORARY TABLE tmp_log (a UInt8) ENGINE = Log;

SELECT '-- a temporary table matched by the engine predicate alone';
SELECT count() > 0 FROM system.table_settings WHERE engine = 'Log' AND database = '';

SELECT '-- the same, where a catalog database holding that engine is read first';
SELECT
    countIf(database = '') > 0 AS keeps_the_temporary_table,
    countIf(database = currentDatabase()) > 0 AS keeps_the_catalog_table
FROM system.table_settings
WHERE (engine = 'Log' AND database = '') OR (engine = 'Memory' AND database = currentDatabase());

SELECT '-- and a predicate about one database only reports no temporary table';
SELECT countIf(database = '')
FROM system.table_settings
WHERE database = currentDatabase() AND engine IN ('Log', 'Memory');

SELECT '-- every shape agrees with filtering after the fact';
SELECT
    (SELECT count() FROM system.table_settings WHERE engine = 'Log' AND database = '')
        = (SELECT countIf(engine = 'Log' AND database = '') FROM system.table_settings),
    (SELECT count() FROM system.table_settings WHERE engine = 'Log')
        = (SELECT countIf(engine = 'Log') FROM system.table_settings);
"

$CLICKHOUSE_CLIENT -q "DROP TABLE catalog_log"
$CLICKHOUSE_CLIENT -q "DROP TABLE catalog_memory"
