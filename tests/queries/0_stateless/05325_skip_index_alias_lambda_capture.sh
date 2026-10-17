#!/usr/bin/env bash
# A skip index that uses an ALIAS column inside a lambda whose parameter would capture the ALIAS expression
# is rejected when written (CREATE, full ATTACH, ALTER), while a table that already stores one keeps loading.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

rejected() { $CLICKHOUSE_CLIENT --send_logs_level fatal --query "$1" 2>&1 | grep -c -m 1 "cannot be expanded inside a lambda"; }

echo '--- CREATE ---'
rejected "CREATE TABLE t_c1 (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(k -> d, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c2 (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1, e UInt32 ALIAS d * 2,
    INDEX i arrayMax(arrayMap(k -> e, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c3 (tup Tuple(a UInt32), arr Array(UInt32), d UInt32 ALIAS tup.a + 1,
    INDEX i arrayMax(arrayMap(tup -> d, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c4 (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(k -> arrayMax(arrayMap(x -> d, arr)), arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c5 (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1,
    INDEX i (k, arrayMax(arrayMap(k -> d, arr))) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c6 (k UInt32, arr Array(Tuple(v UInt32)), \`t.v\` UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(t -> t.v, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c7 (k UInt32, arr Array(Tuple(v UInt32)), \`t.v\` UInt32 ALIAS k + 1,
    w UInt32 ALIAS arrayMax(arrayMap(t -> t.v, arr)), INDEX i w TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"
rejected "CREATE TABLE t_c8 (s String, \`t.v\` String ALIAS upper(s),
    INDEX i(s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(s, arrayStringConcat(arrayMap(t -> lower(t.v), [CAST(tuple('shadow'), 'Tuple(v String)')]), '')))) ENGINE = MergeTree ORDER BY tuple()"

echo '--- not captured ---'
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ok
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS k + 1,
    m UInt32 ALIAS arrayMax(arrayMap(k -> k + 1, arr)),
    INDEX i1 arrayMax(arrayMap(x -> d, arr)) TYPE minmax,
    INDEX i2 arrayMax(arrayMap(d -> d, arr)) TYPE minmax,
    INDEX i3 d TYPE minmax,
    INDEX i4 arrayMax(arrayMap(k -> m, arr)) TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple();
SELECT name, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_ok' AND creation = 'Explicit' ORDER BY name;
INSERT INTO t_ok VALUES (5, [100]);
SELECT arrayMax(arrayMap(x -> d, arr)) FROM t_ok;
"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ok2 (k UInt32, arr Array(Tuple(v UInt32)), \`t.v\` UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(x -> t.v, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple();
SELECT name, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_ok2' AND creation = 'Explicit' ORDER BY name;
"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_ok3 (s String, \`t.v\` String ALIAS upper(s),
    INDEX i(s) TYPE text(tokenizer = 'splitByNonAlpha', preprocessor = concat(s, arrayStringConcat(arrayMap(x -> lower(t.v), [CAST(tuple('shadow'), 'Tuple(v String)')]), '')))) ENGINE = MergeTree ORDER BY tuple();
SELECT name, type FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_ok3' AND creation = 'Explicit' ORDER BY name;
"

echo '--- ALTER ---'
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_alter
(
    k UInt32,
    arr Array(UInt32),
    d UInt32 ALIAS 1,
    e UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(k -> d, arr)) TYPE minmax,
    INDEX j arrayMax(arrayMap(x -> e, arr)) TYPE minmax
)
ENGINE = MergeTree ORDER BY tuple()"
rejected "ALTER TABLE t_alter ADD INDEX n arrayMax(arrayMap(k -> e, arr)) TYPE minmax"
rejected "CREATE INDEX n ON t_alter (arrayMax(arrayMap(k -> e, arr))) TYPE minmax"
rejected "CREATE HYPOTHETICAL INDEX n ON t_alter (arrayMax(arrayMap(k -> e, arr))) TYPE minmax"
$CLICKHOUSE_CLIENT --query "CREATE FUNCTION ${CLICKHOUSE_DATABASE}_amap AS (x, xs) -> arrayMax(arrayMap(k -> x, xs))"
rejected "ALTER TABLE t_alter ADD INDEX n ${CLICKHOUSE_DATABASE}_amap(e, arr) TYPE minmax"
rejected "CREATE HYPOTHETICAL INDEX n ON t_alter (${CLICKHOUSE_DATABASE}_amap(e, arr)) TYPE minmax"
$CLICKHOUSE_CLIENT --query "CREATE HYPOTHETICAL INDEX h ON t_alter (${CLICKHOUSE_DATABASE}_amap(d, arr)) TYPE minmax"
$CLICKHOUSE_CLIENT --query "DROP FUNCTION ${CLICKHOUSE_DATABASE}_amap"
rejected "ALTER TABLE t_alter MODIFY COLUMN d UInt32 ALIAS k + 1"
rejected "ALTER TABLE t_alter RENAME COLUMN k TO x"
$CLICKHOUSE_CLIENT --query "
ALTER TABLE t_alter ADD INDEX n arrayMax(arrayMap(x -> e, arr)) TYPE minmax;
ALTER TABLE t_alter RENAME COLUMN k TO k2;
SELECT name, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_alter' AND creation = 'Explicit' ORDER BY name;
"

echo '--- EXPLAIN WHATIF rebuild ---'
whatif_rejected() { $CLICKHOUSE_CLIENT --query "$1" 2>&1 | grep -c -m 1 "no longer matches the current table schema: .*cannot be expanded inside a lambda"; }
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_whatif_udf (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_whatif_udf (k, arr) SELECT number, [toUInt32(number + 100)] FROM numbers(20);
CREATE TABLE t_whatif_alias (k UInt32, arr Array(UInt32), d UInt32 ALIAS 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 2;
INSERT INTO t_whatif_alias (k, arr) SELECT number, [toUInt32(number + 100)] FROM numbers(20);
"
whatif_rejected "
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_wf AS (x, xs) -> arrayMax(arrayMap(q -> x, xs));
CREATE HYPOTHETICAL INDEX h ON t_whatif_udf (${CLICKHOUSE_DATABASE}_wf(d, arr)) TYPE minmax;
CREATE OR REPLACE FUNCTION ${CLICKHOUSE_DATABASE}_wf AS (x, xs) -> arrayMax(arrayMap(k -> x, xs));
EXPLAIN WHATIF empirical = 0 SELECT * FROM t_whatif_udf WHERE arrayMax(arrayMap(k -> k + 1, arr)) > 115;
"
$CLICKHOUSE_CLIENT --query "DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_wf"
whatif_rejected "
CREATE HYPOTHETICAL INDEX h ON t_whatif_alias (arrayMax(arrayMap(k -> d, arr))) TYPE minmax;
ALTER TABLE t_whatif_alias MODIFY COLUMN d UInt32 ALIAS k + 1;
EXPLAIN WHATIF empirical = 0 SELECT * FROM t_whatif_alias WHERE arrayMax(arrayMap(k -> k + 1, arr)) > 115;
"

echo '--- automatic minmax index ---'
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_auto
(
    k UInt32,
    y UInt32,
    arr Array(UInt32),
    e UInt32 ALIAS k + 1,
    f UInt32 ALIAS y + 1,
    d UInt32 ALIAS arrayMax(arrayMap(k -> e, arr)),
    m UInt32 ALIAS arrayMax(arrayMap(x -> e, arr)),
    g UInt32 ALIAS arrayMax(arrayMap(z -> f, arr)),
    s String,
    es String ALIAS s,
    n UInt64 ALIAS arrayMax(arrayMap(s -> length(es), arr))
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_auto ADD COLUMN d2 UInt32 ALIAS arrayMin(arrayMap(k -> e, arr));
ALTER TABLE t_auto ADD COLUMN m2 UInt32 ALIAS arrayMin(arrayMap(x -> e, arr));
ALTER TABLE t_auto ADD COLUMN n2 UInt64 ALIAS arrayMin(arrayMap(s -> length(es), arr));
ALTER TABLE t_auto RENAME COLUMN y TO z;
SELECT name, expr FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_auto'
    AND name IN ('auto_minmax_index_d', 'auto_minmax_index_d2', 'auto_minmax_index_m', 'auto_minmax_index_m2', 'auto_minmax_index_n', 'auto_minmax_index_n2')
ORDER BY name;
INSERT INTO t_auto (k, z, arr, s) VALUES (5, 1, [100], 'abc');
SELECT n, n2 FROM t_auto;
"

echo '--- stored definition ---'
WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"
rejected_local() { $CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --query "$1" 2>&1 | grep -c -m 1 "cannot be expanded inside a lambda"; }

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --query "
CREATE DATABASE db;
CREATE TABLE db.t (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(x -> d, arr)) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple();
"

# The form a server without the check stored.
metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/arrayMap(x -> d, arr)/arrayMap(k -> d, arr)/' "${metadata_file}"
grep -c -F 'arrayMap(k -> d, arr)' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" --query "
SELECT expr FROM system.data_skipping_indices WHERE database = 'db' AND table = 't' AND name = 'i';
INSERT INTO db.t VALUES (5, [100]);
INSERT INTO db.t VALUES (6, [100]);
OPTIMIZE TABLE db.t FINAL;
ALTER TABLE db.t CLEAR INDEX i SETTINGS mutations_sync = 2;
ALTER TABLE db.t MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
ALTER TABLE db.t ADD COLUMN z UInt8;
ALTER TABLE db.t ADD INDEX j arrayMax(arrayMap(x -> d, arr)) TYPE minmax;
DETACH TABLE db.t;
ATTACH TABLE db.t;
SELECT k, arrayMax(arrayMap(k -> d, arr)) FROM db.t ORDER BY k;
SELECT name, expr FROM system.data_skipping_indices WHERE database = 'db' AND table = 't' AND creation = 'Explicit' ORDER BY name;
"

rejected_local "ALTER TABLE db.t DROP INDEX i, ADD INDEX i arrayMax(arrayMap(k -> d, arr)) TYPE minmax"
rejected_local "ALTER TABLE db.t ADD INDEX n arrayMin(arrayMap(k -> d, arr)) TYPE minmax"
rejected_local "CREATE TABLE db.t2 AS db.t"
rejected_local "ATTACH TABLE db.t3 UUID '05325000-0000-0000-0000-000000000003' (k UInt32, arr Array(UInt32), d UInt32 ALIAS k + 1,
    INDEX i arrayMax(arrayMap(k -> d, arr)) TYPE minmax) ENGINE = MergeTree ORDER BY tuple()"

rm -rf "${WORKING_DIR}"
