-- A filter over a LowCardinality column is recognized as always true or always false for a block when
-- every row of it maps to the same dictionary key, which happens whenever the function maps every key
-- of the dictionary to the same result. The recognition is per block, so it must never change a result.

SET allow_suspicious_low_cardinality_types = 1;

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
SELECT count(), countIf(lc != ''), countIf(lc = ''), countIf(lc_nullable IS NULL), sum(intDiv(100, lc_num))
FROM t_lc_single_value;

SELECT 'constant false filter';
SELECT count() FROM t_lc_single_value WHERE lc != '' SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc != '';

SELECT 'constant true filter';
SELECT count() FROM t_lc_single_value WHERE lc = '' SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc = '' WHERE id % 2 = 0;
SELECT count() FROM t_lc_single_value PREWHERE lc = '' AND id < 500 SETTINGS enable_multiple_prewhere_read_steps = 1;

SELECT 'non bool filter';
-- The single value has to be converted to bool, and a NULL there has to filter the row out.
DROP TABLE IF EXISTS t_lc_not_bool;
CREATE TABLE t_lc_not_bool (value Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_lc_not_bool SELECT 1 FROM numbers(20);
SELECT sum(*) FROM t_lc_not_bool PREWHERE toLowCardinality(max2(0, value::Int64 - 10));
SELECT sum(*) FROM t_lc_not_bool PREWHERE toLowCardinality(toNullable(max2(0, value::Int64 - 10)));
SELECT sum(*) FROM t_lc_not_bool PREWHERE toLowCardinality(materialize(CAST(NULL, 'Nullable(UInt8)')));
SELECT count() FROM t_lc_single_value PREWHERE toNullable(toInt64(lc = ''));

SELECT 'mostly empty';
-- One rare non-default value is enough to keep the filter from being constant, and the row must survive.
DROP TABLE IF EXISTS t_lc_mostly_empty;
CREATE TABLE t_lc_mostly_empty (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_mostly_empty SELECT number, if(number = 500, 'x', '') FROM numbers(1000);

SELECT id, lc FROM t_lc_mostly_empty WHERE lc != '';
SELECT id, lc FROM t_lc_mostly_empty PREWHERE lc != '';
-- The rare row lands in a block of its own for a small enough block size, so both the constant and the
-- non constant shape of the filter are exercised, and the answer must not depend on where it lands.
SELECT count() FROM t_lc_mostly_empty WHERE lc != '' SETTINGS max_block_size = 8;
SELECT count() FROM t_lc_mostly_empty WHERE lc != '' SETTINGS max_block_size = 65536;

SELECT 'several keys with the same result';
-- Every row maps to the same result from a dictionary of more than one key, so the detection has to be
-- about the indexes being equal rather than about the dictionary holding a single entry.
DROP TABLE IF EXISTS t_lc_two_values;
CREATE TABLE t_lc_two_values (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_two_values SELECT number, if(number % 2, 'a', 'b') FROM numbers(1000);

SELECT count() FROM t_lc_two_values WHERE lc != 'c';

SELECT 'zero or one row';
-- A filter that is constant for a block must not be mistaken for one that is constant for the query.
-- Headers are evaluated on zero rows and plan time constants on one row, over a dictionary that holds
-- only the default value, so folding either of them would drop every row of the WHERE.
SELECT count() FROM (SELECT toLowCardinality(if(number = 3, 'x', '')) AS lc FROM numbers(1000)) WHERE lc = 'x';
SELECT count() FROM (SELECT toLowCardinality(materialize('')) AS lc FROM numbers(1)) WHERE lc = '';

SELECT 'correlated subquery';
-- Decorrelation evaluates the condition on one row at plan time, which must not be folded either.
-- Correlated subqueries need the analyzer, so pin it for this query.
DROP TABLE IF EXISTS t_lc_correlated;
CREATE TABLE t_lc_correlated (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_correlated VALUES (0, ''), (1, 'a');
SELECT id FROM t_lc_correlated WHERE EXISTS (SELECT 1 FROM numbers(3) WHERE t_lc_correlated.lc != '')
ORDER BY id SETTINGS enable_analyzer = 1;

SELECT 'constant and non constant parts in one query';
-- The first part has a single value dictionary and the second one does not, so both shapes of the filter
-- are reached by one query and must give one consistent answer.
DROP TABLE IF EXISTS t_lc_mixed_parts;
CREATE TABLE t_lc_mixed_parts (id UInt64, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_lc_mixed_parts;
INSERT INTO t_lc_mixed_parts SELECT number, '' FROM numbers(1000);
INSERT INTO t_lc_mixed_parts SELECT number + 1000, if(number % 2, 'x', '') FROM numbers(1000);

SELECT count() FROM t_lc_mixed_parts PREWHERE lc != '';
SELECT count() FROM t_lc_mixed_parts WHERE lc != '' SETTINGS optimize_move_to_prewhere = 0;

SELECT 'isConstant is a property of the query, not of the block';
-- Recognizing the filter must not make `isConstant` depend on the part or on the block size.
SELECT DISTINCT isConstant(lc != '') FROM t_lc_mixed_parts;
SELECT DISTINCT isConstant(lc != '') FROM t_lc_single_value SETTINGS max_block_size = 8;
SELECT DISTINCT isConstant(lc != '') FROM t_lc_single_value SETTINGS max_block_size = 65536;
-- A constant known during analysis is still reported as one.
SELECT isConstant(toLowCardinality('') != ''), isConstant(materialize(toLowCardinality('')) != '');

SELECT 'mutation';
-- A mutation filters rows with the same machinery, in both directions.
ALTER TABLE t_lc_mostly_empty UPDATE lc = 'z' WHERE lc = 'q' SETTINGS mutations_sync = 2;
SELECT count(), countIf(lc = 'z') FROM t_lc_mostly_empty;
ALTER TABLE t_lc_single_value UPDATE lc_num = 7 WHERE lc = '' SETTINGS mutations_sync = 2;
SELECT count(), uniqExact(lc_num), any(lc_num) FROM t_lc_single_value;

DROP TABLE t_lc_not_bool;
DROP TABLE t_lc_mixed_parts;
DROP TABLE t_lc_mostly_empty;
DROP TABLE t_lc_two_values;
DROP TABLE t_lc_correlated;
DROP TABLE t_lc_single_value;
