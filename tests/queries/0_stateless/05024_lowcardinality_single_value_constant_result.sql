-- A function over a LowCardinality column returns a constant when it maps every dictionary key to the
-- same value, so it costs the size of the dictionary instead of one index per row.

SET allow_suspicious_low_cardinality_types = 1;
-- Whether the result is constant depends on the dictionary of the block, so keep the block size fixed.
SET max_block_size = 65536;

DROP TABLE IF EXISTS t_lc_single_value;

CREATE TABLE t_lc_single_value
(
    id UInt64,
    lc LowCardinality(String),
    lc_nullable LowCardinality(Nullable(String)),
    lc_num LowCardinality(UInt32)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_lc_single_value SELECT number, '', NULL, 5 FROM numbers(1000);

SELECT 'single value dictionary';
SELECT DISTINCT isConstant(lc != ''), isConstant(concat(lc, 'x')) FROM t_lc_single_value;
-- intDiv cannot be executed on the dictionary default, so it goes through the minimal dictionary path.
SELECT DISTINCT isConstant(intDiv(100, lc_num)) FROM t_lc_single_value;
-- A LowCardinality(Nullable) dictionary always holds both NULL and the default, so a column of only
-- NULLs stays non-constant unless the function maps NULL and the default to the same value.
SELECT DISTINCT isConstant(lc_nullable IS NULL), isConstant(ifNull(lc_nullable, '') = '') FROM t_lc_single_value;

SELECT count(), countIf(lc != ''), countIf(lc = ''), countIf(lc_nullable IS NULL), sum(intDiv(100, lc_num))
FROM t_lc_single_value;
SELECT DISTINCT lc, concat(lc, 'x'), lc_nullable, lc_nullable != '', intDiv(100, lc_num) FROM t_lc_single_value;
SELECT lc != '' AS x, count() FROM t_lc_single_value GROUP BY x ORDER BY x;

SELECT 'constant false filter';
SELECT count() FROM t_lc_single_value WHERE lc != '' SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc != '';

SELECT 'constant true filter';
SELECT count() FROM t_lc_single_value WHERE lc = '' SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc = '' WHERE id % 2 = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc = '' AND id < 500 SETTINGS enable_multiple_prewhere_read_steps = 1;

SELECT 'non bool filter';
-- The filter is a LowCardinality of a wide or nullable number, so preprocessFilterColumn has to convert
-- it to bool and apply the null map - both when the filter is a constant and when it is not.
DROP TABLE IF EXISTS t_lc_not_bool;
CREATE TABLE t_lc_not_bool (value Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_not_bool SELECT 1 FROM numbers(20);
SELECT sum(*) FROM t_lc_not_bool PREWHERE toLowCardinality(max2(0, value::Int64 - 10));
SELECT sum(*) FROM t_lc_not_bool PREWHERE toLowCardinality(toNullable(max2(0, value::Int64 - 10)));
SELECT sum(*) FROM t_lc_not_bool WHERE toLowCardinality(toNullable(max2(0, value::Int64 - 10)));
SELECT count() FROM t_lc_single_value PREWHERE toNullable(toInt64(lc = ''));

SELECT 'zero or one row';
-- Headers are built by running the actions on zero rows, over a dictionary that holds only the default
-- value. A constant there would be read as a compile time constant and drop every row of the WHERE.
SELECT count() FROM (SELECT toLowCardinality(if(number = 3, 'x', '')) AS lc FROM numbers(1000)) WHERE lc = 'x';
-- Plan time constants are evaluated on one row, so one row must not produce a constant either. It is
-- also worth nothing there, unlike two rows.
SELECT isConstant(toLowCardinality(materialize('')) != '') FROM numbers(1);
SELECT DISTINCT isConstant(toLowCardinality(materialize('')) != '') FROM numbers(2);

SELECT 'correlated subquery';
-- Decorrelation evaluates the condition on one row at plan time, which must not be folded either.
-- Correlated subqueries need the analyzer, so pin it for this query only.
DROP TABLE IF EXISTS t_lc_correlated;
CREATE TABLE t_lc_correlated (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_correlated VALUES (0, ''), (1, 'a');
SELECT id FROM t_lc_correlated WHERE EXISTS (SELECT 1 FROM numbers(3) WHERE t_lc_correlated.lc != '')
ORDER BY id SETTINGS enable_analyzer = 1;
-- Composite functions such as nullIf run their nested calls with a dry run flag of their own, so the
-- guard cannot rest on that flag.
SELECT id FROM t_lc_correlated WHERE EXISTS (SELECT 1 FROM numbers(3) WHERE nullIf(t_lc_correlated.lc, '') IS NULL)
ORDER BY id SETTINGS enable_analyzer = 1;

SELECT 'several keys with the same result';
DROP TABLE IF EXISTS t_lc_two_values;
CREATE TABLE t_lc_two_values (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_two_values SELECT number, if(number % 2, 'a', 'b') FROM numbers(1000);

SELECT DISTINCT isConstant(lc != 'c') FROM t_lc_two_values;

SELECT 'several keys with different results';
SELECT DISTINCT isConstant(lc = 'a') FROM t_lc_two_values;
-- The default key is always in the dictionary, so a column with no empty value is not detected as constant.
SELECT DISTINCT isConstant(lc != '') FROM t_lc_two_values;
-- The minimal dictionary path keeps only the keys that are used, so it is not constant either.
SELECT DISTINCT isConstant(intDiv(100, lc_num))
FROM (SELECT toLowCardinality(if(number % 2, 2, 5)) AS lc_num FROM numbers(1000));
SELECT countIf(lc != 'c'), countIf(lc = 'a'), countIf(lc != '') FROM t_lc_two_values;

SELECT 'mostly empty';
-- One rare non-default value is enough to keep the dictionary from collapsing, and the row must survive.
DROP TABLE IF EXISTS t_lc_mostly_empty;
CREATE TABLE t_lc_mostly_empty (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_mostly_empty SELECT number, if(number = 500, 'x', '') FROM numbers(1000);

SELECT id, lc FROM t_lc_mostly_empty WHERE lc != '';

SELECT 'constant and non constant parts in one query';
-- The first part is constant, the second one is not, so both shapes reach the same aggregation.
DROP TABLE IF EXISTS t_lc_mixed_parts;
CREATE TABLE t_lc_mixed_parts (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_lc_mixed_parts;
INSERT INTO t_lc_mixed_parts SELECT number, '' FROM numbers(1000);
INSERT INTO t_lc_mixed_parts SELECT number + 1000, if(number % 2, 'x', '') FROM numbers(1000);

SELECT DISTINCT isConstant(lc != '') FROM t_lc_mixed_parts ORDER BY 1;
SELECT count() FROM t_lc_mixed_parts PREWHERE lc != '';
SELECT concat(lc, 'y') AS x, count() FROM t_lc_mixed_parts GROUP BY x ORDER BY x;

SELECT 'union all of a constant and a non constant branch';
SELECT x, count() FROM (SELECT lc != '' AS x FROM t_lc_single_value UNION ALL SELECT lc != '' AS x FROM t_lc_two_values)
GROUP BY x ORDER BY x;

SELECT 'sorting and window';
SELECT lc != '' AS x FROM t_lc_single_value ORDER BY x LIMIT 2;
SELECT DISTINCT count() OVER (PARTITION BY lc != '') FROM t_lc_single_value;

SELECT 'join on a constant key';
SELECT count() FROM t_lc_single_value AS a JOIN t_lc_correlated AS b ON concat(a.lc, 'k') = concat(b.lc, 'k');

SELECT 'distributed';
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_lc_single_value) WHERE lc != '';
-- Here the constant crosses the wire, and NativeWriter has to materialize it to serialize it.
SELECT DISTINCT x FROM (SELECT lc != '' AS x FROM remote('127.0.0.{1,2}', currentDatabase(), t_lc_single_value));

SELECT 'writing a constant into a part';
-- A part cannot hold a constant column, so the select list, the materialized column expression and the
-- mutation all have to materialize it.
DROP TABLE IF EXISTS t_lc_insert_target;
CREATE TABLE t_lc_insert_target (x LowCardinality(UInt8), s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_insert_target SELECT lc != '', concat(lc, 'x') FROM t_lc_single_value;
SELECT count(), uniqExact(x), uniqExact(s), any(x), any(s) FROM t_lc_insert_target;

DROP TABLE IF EXISTS t_lc_materialized;
CREATE TABLE t_lc_materialized (lc LowCardinality(String), lc_m LowCardinality(String) MATERIALIZED concat(lc, 'm'))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_materialized SELECT '' FROM numbers(1000);
SELECT count(), uniqExact(lc_m), any(lc_m) FROM t_lc_materialized;
ALTER TABLE t_lc_materialized UPDATE lc = concat(lc, '') WHERE lc = '' SETTINGS mutations_sync = 2;
SELECT count(), uniqExact(lc), uniqExact(lc_m) FROM t_lc_materialized;

DROP TABLE t_lc_materialized;
DROP TABLE t_lc_not_bool;
DROP TABLE t_lc_insert_target;
DROP TABLE t_lc_mixed_parts;
DROP TABLE t_lc_mostly_empty;
DROP TABLE t_lc_two_values;
DROP TABLE t_lc_correlated;
DROP TABLE t_lc_single_value;
