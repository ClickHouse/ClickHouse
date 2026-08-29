-- `AggregateFunctionFactory::get` strips `LowCardinality` from the argument types, so
-- `ColumnAggregateFunction::type_string` never carries it, while `DataTypeAggregateFunction::getName`
-- - and therefore the field produced by `getDefault` - does. An INSERT that omits the state column
-- builds its default through `IDataType::createColumnConst` -> `ColumnAggregateFunction::insert`,
-- which used to reject the column's own default value.

DROP TABLE IF EXISTS t_agg_lc_argmaxif;
DROP TABLE IF EXISTS t_agg_lc_plain;
DROP TABLE IF EXISTS t_agg_lc_merge;

SELECT '-- the reported case: argMaxIf over LowCardinality(Nullable(String))';

CREATE TABLE t_agg_lc_argmaxif
(
    id UInt64,
    v AggregateFunction(argMaxIf, LowCardinality(Nullable(String)), DateTime, Bool)
)
ENGINE = AggregatingMergeTree ORDER BY id;

INSERT INTO t_agg_lc_argmaxif (id) VALUES (1);

SELECT count() FROM t_agg_lc_argmaxif;
SELECT DISTINCT toTypeName(v) FROM t_agg_lc_argmaxif;

SELECT '-- not specific to argMax, to the -If combinator, or to Nullable';

CREATE TABLE t_agg_lc_plain
(
    id UInt64,
    s AggregateFunction(max, LowCardinality(String)),
    u AggregateFunction(uniq, LowCardinality(Nullable(String))),
    g AggregateFunction(groupArray, LowCardinality(String))
)
ENGINE = AggregatingMergeTree ORDER BY id;

INSERT INTO t_agg_lc_plain (id) VALUES (1);

SELECT count() FROM t_agg_lc_plain;
SELECT DISTINCT toTypeName(s), toTypeName(u), toTypeName(g) FROM t_agg_lc_plain;

SELECT '-- a real state still merges with a default-constructed one';

CREATE TABLE t_agg_lc_merge
(
    id UInt64,
    s AggregateFunction(max, LowCardinality(String))
)
ENGINE = AggregatingMergeTree ORDER BY id;

INSERT INTO t_agg_lc_merge (id) VALUES (1);
INSERT INTO t_agg_lc_merge SELECT 1, maxState(CAST('b', 'LowCardinality(String)'));
INSERT INTO t_agg_lc_merge SELECT 1, maxState(CAST('a', 'LowCardinality(String)'));

SELECT maxMerge(s) FROM t_agg_lc_merge;

SELECT '-- the same default is reachable through a DEFAULT-less column of a plain MergeTree';

DROP TABLE IF EXISTS t_agg_lc_mt;
CREATE TABLE t_agg_lc_mt
(
    id UInt64,
    s AggregateFunction(max, LowCardinality(String))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_agg_lc_mt (id) SELECT number FROM numbers(3);
SELECT count() FROM t_agg_lc_mt;

DROP TABLE t_agg_lc_argmaxif;
DROP TABLE t_agg_lc_plain;
DROP TABLE t_agg_lc_merge;
DROP TABLE t_agg_lc_mt;
