-- Window PARTITION BY and ORDER BY over an aggregate function state used to be accepted by at
-- least one analyzer. Both are refused now, like top-level ORDER BY already refuses them.

SET allow_experimental_qbit_type = 1;
SET allow_experimental_variant_type = 1;
SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS t_wpb_state;
CREATE TABLE t_wpb_state
(
    id UInt8,
    st AggregateFunction(uniq, UInt64),
    arr Array(AggregateFunction(uniq, UInt64)),
    tup Tuple(a AggregateFunction(uniq, UInt64)),
    mp Map(String, AggregateFunction(uniq, UInt64)),
    deep Array(Tuple(a AggregateFunction(uniq, UInt64))),
    saf SimpleAggregateFunction(sum, UInt64),
    s String,
    lc LowCardinality(String),
    nl Nullable(String)
) ENGINE = Memory;

INSERT INTO t_wpb_state
SELECT number, uniqState(number), [uniqState(number)], tuple(uniqState(number)),
       map('k', uniqState(number)), [tuple(uniqState(number))], number,
       toString(number % 3), toString(number % 3), toString(number % 3)
FROM numbers(6) GROUP BY number;

SELECT '-- the analyzer: refused';
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY st) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY arr) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY tup) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY mp) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY deep) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY id, st) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }

SELECT '-- old analyzer: refused';
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (ORDER BY st) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (ORDER BY arr) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (ORDER BY tup) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (ORDER BY mp) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (ORDER BY deep) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY id ORDER BY st) FROM t_wpb_state); -- { serverError ILLEGAL_COLUMN }

SELECT '-- window ORDER BY over a state is refused, old analyzer';
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER w FROM t_wpb_state WINDOW w AS (ORDER BY st)); -- { serverError ILLEGAL_COLUMN }
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (ORDER BY saf) FROM t_wpb_state);
SELECT count() FROM (SELECT row_number() OVER (ORDER BY s) FROM t_wpb_state);
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY saf) FROM t_wpb_state);
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY s) FROM t_wpb_state);
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY lc) FROM t_wpb_state);
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY nl) FROM t_wpb_state);

SELECT '-- comparable types are untouched, old analyzer';
SET enable_analyzer = 1;
SELECT count() FROM (SELECT st FROM t_wpb_state GROUP BY st);
SELECT count() FROM (SELECT DISTINCT st FROM t_wpb_state);
SET enable_analyzer = 1;
SELECT st FROM t_wpb_state ORDER BY st; -- { serverError ILLEGAL_COLUMN }
DROP TABLE IF EXISTS t_wpb_qbit;
CREATE TABLE t_wpb_qbit (id UInt8, q QBit(BFloat16, 16), qa Array(QBit(BFloat16, 16))) ENGINE = Memory;
INSERT INTO t_wpb_qbit
SELECT number, arrayMap(x -> toFloat32(number), range(16))::QBit(BFloat16, 16),
       [arrayMap(x -> toFloat32(number), range(16))::QBit(BFloat16, 16)]
FROM numbers(6);
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY q) FROM t_wpb_qbit);
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY qa) FROM t_wpb_qbit);
-- The window ORDER BY guard is aggregate-state-specific, so it does not widen the old analyzer's
-- acceptance of a QBit sort key. The analyzer refuses it for being incomparable, as it already did.
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (ORDER BY q) FROM t_wpb_qbit); -- { serverError ILLEGAL_COLUMN }

SELECT '-- the pre-existing Dynamic/Variant gate is undisturbed';
DROP TABLE IF EXISTS t_wpb_dyn;
CREATE TABLE t_wpb_dyn (d Dynamic, val UInt64) ENGINE = Memory;
INSERT INTO t_wpb_dyn VALUES (1, 10), (2, 20), ('str', 30);
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY d) FROM t_wpb_dyn); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY d) FROM t_wpb_dyn) SETTINGS allow_suspicious_types_in_group_by = 1;
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY CAST(st, 'Dynamic')) FROM t_wpb_state) SETTINGS allow_suspicious_types_in_group_by = 1;
DROP TABLE IF EXISTS t_wpb_var;
CREATE TABLE t_wpb_var (v Variant(AggregateFunction(uniq, UInt64), String), val UInt64) ENGINE = Memory;
INSERT INTO t_wpb_var SELECT uniqState(number)::Variant(AggregateFunction(uniq, UInt64), String), number FROM numbers(3) GROUP BY number;
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY v) FROM t_wpb_var) SETTINGS allow_suspicious_types_in_group_by = 1; -- { serverError ILLEGAL_COLUMN }
DROP TABLE IF EXISTS t_wpb_saf;
CREATE TABLE t_wpb_saf (id UInt8, sn SimpleAggregateFunction(any, AggregateFunction(uniq, UInt64))) ENGINE = Memory;
INSERT INTO t_wpb_saf SELECT number, uniqState(number) FROM numbers(6) GROUP BY number;
SET enable_analyzer = 1;
SELECT count() FROM (SELECT row_number() OVER (PARTITION BY sn) FROM t_wpb_saf); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM (SELECT row_number() OVER (ORDER BY sn) FROM t_wpb_saf); -- { serverError ILLEGAL_COLUMN }
SELECT id FROM t_wpb_saf ORDER BY sn; -- { serverError ILLEGAL_COLUMN }

DROP TABLE t_wpb_state;
DROP TABLE t_wpb_qbit;
DROP TABLE t_wpb_dyn;
DROP TABLE t_wpb_var;
DROP TABLE t_wpb_saf;
