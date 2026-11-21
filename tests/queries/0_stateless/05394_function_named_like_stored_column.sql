-- A column named like an expression (as CREATE TABLE ... AS SELECT a = b names it) is not that expression.

-- { echoOn }

CREATE TABLE t (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, `notEquals(a, b)` String,
    x UInt8 ALIAS (a = b) AND (`equals(a, b)` = 7)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mem (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8) ENGINE = Memory;
INSERT INTO t VALUES (1, 1, 10, 7, 'p'), (1, 2, 20, 7, 'q'), (3, 3, 30, 9, 'r');
INSERT INTO t_mem SELECT a, b, k, `equals(a, b)` FROM t;

SELECT k, `equals(a, b)` FROM t PREWHERE a = b ORDER BY k;
SELECT k, `equals(a, b)` FROM t_mem PREWHERE a = b ORDER BY k;
SELECT k, `notEquals(a, b)` FROM t PREWHERE a != b ORDER BY k;
SELECT k FROM t PREWHERE a = b AND `equals(a, b)` = 7 ORDER BY k;
SELECT k FROM t PREWHERE `equals(a, b)` = 7 AND a = b ORDER BY k;
SELECT k, `equals(a, b)` FROM t ORDER BY k SETTINGS additional_table_filters = {'t': 'a = b'};
SELECT k FROM t ORDER BY k SETTINGS additional_table_filters = {'t': 'a = b AND `equals(a, b)` = 7'};
SELECT k, x FROM t ORDER BY k;

CREATE TABLE t_merge (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, x UInt8 ALIAS a = b)
    ENGINE = Merge(currentDatabase(), '^t_mem$');
SELECT k, x, `equals(a, b)` FROM t_merge ORDER BY k;

CREATE TABLE t_child (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, x UInt8 ALIAS a = b) ENGINE = Memory;
INSERT INTO t_child (a, b, k, `equals(a, b)`) VALUES (1, 1, 10, 7), (1, 2, 20, 7), (3, 3, 30, 9);
CREATE TABLE t_merge_child (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, x UInt8)
    ENGINE = Merge(currentDatabase(), '^t_child$');
SELECT k, x, `equals(a, b)` FROM t_merge_child ORDER BY k;
SELECT k, `equals(a, b)`, x FROM t_merge_child ORDER BY k;

CREATE TABLE t_mut (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, c UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_mut VALUES (1, 1, 10, 7, 5), (1, 2, 20, 7, 5), (3, 3, 30, 9, 5);
ALTER TABLE t_mut UPDATE c = (a = b) WHERE 1 SETTINGS mutations_sync = 2;
SELECT k, c FROM t_mut ORDER BY k;
ALTER TABLE t_mut DELETE WHERE a = b SETTINGS mutations_sync = 2;
SELECT k FROM t_mut ORDER BY k;

CREATE TABLE t_part (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8) ENGINE = MergeTree PARTITION BY `equals(a, b)` ORDER BY k;
INSERT INTO t_part VALUES (1, 1, 10, 0), (1, 2, 20, 1), (3, 3, 30, 1), (4, 5, 40, 0);
ALTER TABLE t_part DELETE WHERE a = b AND `equals(a, b)` = 0 SETTINGS mutations_sync = 2;
SELECT k FROM t_part ORDER BY k;

CREATE TABLE t_def (a UInt8, b UInt8, k UInt64, `equals(a, b)` UInt8, c UInt8 DEFAULT a = b, m UInt8 MATERIALIZED a = b)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO t_def (a, b, k, `equals(a, b)`) VALUES (1, 1, 10, 7), (1, 2, 20, 7), (3, 3, 30, 9);
SELECT k, c, m FROM t_def ORDER BY k;

CREATE TABLE t_inf1 (a UInt8, b UInt8, `equals(a, b)` UInt16, x DEFAULT a = b, y DEFAULT `equals(a, b)`) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_inf2 (a UInt8, b UInt8, `equals(a, b)` UInt16, y DEFAULT `equals(a, b)`, x DEFAULT a = b) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_inf1 (a, b, `equals(a, b)`) VALUES (1, 1, 300), (1, 2, 301);
INSERT INTO t_inf2 (a, b, `equals(a, b)`) VALUES (1, 1, 300), (1, 2, 301);
SELECT toTypeName(x), toTypeName(y), x, y FROM t_inf1 ORDER BY b;
SELECT toTypeName(x), toTypeName(y), x, y FROM t_inf2 ORDER BY b;

CREATE TABLE t_idx (a UInt8, b UInt8, `equals(a, b)` UInt8) ENGINE = MergeTree ORDER BY `equals(a, b)`
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_idx VALUES (1, 1, 0), (1, 2, 0), (3, 3, 1), (4, 5, 1), (6, 6, 2);
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_idx, `equals(a, b)` = 0 AND a = b);
SELECT ranges FROM mergeTreeAnalyzeIndexes(currentDatabase(), t_idx, indexHint(`equals(a, b)` = 0 AND a = b));

CREATE TABLE t_lam (a UInt8, arr Array(UInt8), `x UInt8 -> equals(x, a)` UInt8, d UInt8 DEFAULT arrayExists(x -> x = a, arr))
    ENGINE = MergeTree ORDER BY a;
INSERT INTO t_lam (a, arr, `x UInt8 -> equals(x, a)`) VALUES (1, [1, 2], 7), (3, [1, 2], 7);
SELECT a, d, `x UInt8 -> equals(x, a)` FROM t_lam ORDER BY a;

CREATE TABLE t_lam2 (a UInt8, arr Array(UInt8), `equals(x, a)` UInt8,
    c Array(UInt8) DEFAULT arrayMap(x -> x = a, arr), s Array(UInt8) DEFAULT arrayMap(x -> `equals(x, a)`, arr))
    ENGINE = MergeTree ORDER BY a;
INSERT INTO t_lam2 (a, arr, `equals(x, a)`) VALUES (1, [1, 2], 7);
SELECT c, s FROM t_lam2;
ALTER TABLE t_lam2 UPDATE c = arrayMap(x -> `equals(x, a)`, arr), s = arrayMap(x -> x = a, arr) WHERE 1 SETTINGS mutations_sync = 2;
SELECT c, s FROM t_lam2;

-- { echoOff }

DROP TABLE t_merge;
DROP TABLE t_merge_child;
DROP TABLE t;
DROP TABLE t_mem;
DROP TABLE t_child;
DROP TABLE t_mut;
DROP TABLE t_part;
DROP TABLE t_def;
DROP TABLE t_inf1;
DROP TABLE t_inf2;
DROP TABLE t_idx;
DROP TABLE t_lam;
DROP TABLE t_lam2;
