-- Tags: shard

-- The nested set operations below need the analyzer: the old one rejects the same queries
-- with NO_COMMON_TYPE before a plan exists.
SET enable_analyzer = 1;

SELECT 'plain select';
SELECT * FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT 10 AS n) EXCEPT ALL SELECT -9223372036854775807 AS n) WHERE (n = (SELECT n));

SELECT 'explain pipeline';
SELECT count() > 0 FROM (EXPLAIN PIPELINE graph = 1
    SELECT * FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT 10 AS n) EXCEPT ALL SELECT -9223372036854775807 AS n) WHERE (n = (SELECT n)));

SELECT 'declared type';
SELECT DISTINCT toTypeName(n) FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT 10 AS n) EXCEPT ALL SELECT -9223372036854775807 AS n);

SELECT 'plan header agrees with the declared type';
SELECT count() > 0 FROM (EXPLAIN header = 1
    SELECT * FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT 10 AS n) EXCEPT ALL SELECT -9223372036854775807 AS n))
WHERE explain ILIKE '%Header: n Variant(Int64, UInt64)%';

SELECT 'materialized branches';
SELECT * FROM (((SELECT materialize(9223372036854775806) AS n) EXCEPT ALL SELECT materialize(10) AS n) EXCEPT ALL SELECT materialize(-9223372036854775807) AS n) WHERE (n = (SELECT n));

SELECT 'intersect';
SELECT count() FROM (((SELECT 9223372036854775806 AS n) INTERSECT ALL SELECT 10 AS n) INTERSECT ALL SELECT -9223372036854775807 AS n) WHERE (n = (SELECT n));

SELECT 'wrapped in Nullable';
SELECT * FROM (((SELECT toNullable(9223372036854775806) AS n) EXCEPT ALL SELECT toNullable(10) AS n) EXCEPT ALL SELECT toNullable(-9223372036854775807) AS n) WHERE (n = (SELECT n));

SELECT 'wrapped in LowCardinality';
SELECT * FROM (((SELECT toLowCardinality(9223372036854775806) AS n) EXCEPT ALL SELECT toLowCardinality(10) AS n) EXCEPT ALL SELECT toLowCardinality(-9223372036854775807) AS n) WHERE (n = (SELECT n));

SELECT 'wrapped in Array';
SELECT * FROM (((SELECT [9223372036854775806] AS n) EXCEPT ALL SELECT [10] AS n) EXCEPT ALL SELECT [-9223372036854775807] AS n) WHERE (n = (SELECT n));

SELECT 'wrapped in Tuple';
SELECT * FROM (((SELECT tuple(9223372036854775806) AS n) EXCEPT ALL SELECT tuple(10) AS n) EXCEPT ALL SELECT tuple(-9223372036854775807) AS n) WHERE (n = (SELECT n));

SELECT 'wrapped in Map';
SELECT * FROM (((SELECT map('k', 9223372036854775806) AS n) EXCEPT ALL SELECT map('k', 10) AS n) EXCEPT ALL SELECT map('k', -9223372036854775807) AS n) WHERE (n = (SELECT n));

-- Controls. Nothing below reaches the divergence, and the fix must leave every one of them alone.

SELECT 'control: explicit casts';
SELECT DISTINCT toTypeName(n) FROM (((SELECT 9223372036854775806::UInt64 AS n) EXCEPT ALL SELECT 10::UInt8 AS n) EXCEPT ALL SELECT -9223372036854775807::Int64 AS n);
SELECT * FROM (((SELECT 9223372036854775806::UInt64 AS n) EXCEPT ALL SELECT 10::UInt8 AS n) EXCEPT ALL SELECT -9223372036854775807::Int64 AS n) WHERE (n = (SELECT n));

SELECT 'control: union all';
SELECT DISTINCT toTypeName(n) FROM (((SELECT 9223372036854775806 AS n) UNION ALL SELECT 10 AS n) UNION ALL SELECT -9223372036854775807 AS n);

-- The conversion is inserted in one direction only: it drops the flag to match the common header
-- and never adds one. A mixed union folds to a different type depending on branch order, and both
-- orders must keep the type they resolve to today.
SELECT 'control: mixed branches, flagged first';
SELECT DISTINCT toTypeName(n) FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT -1::Int64 AS n);
SELECT count() FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT -1::Int64 AS n) WHERE (n = (SELECT n));

SELECT 'control: mixed branches, unflagged first';
SELECT DISTINCT toTypeName(n) FROM (((SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT -1::Int64 AS n);
SELECT count() FROM (((SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT -1::Int64 AS n) WHERE (n = (SELECT n));

-- A CAST converts a whole column, so a container that would drop one leaf's flag and add another's
-- is left untouched.
SELECT 'control: tuple with flags in both directions';
SELECT DISTINCT toTypeName(n) FROM (((SELECT tuple(9223372036854775806, 1::UInt64) AS n) EXCEPT ALL SELECT tuple(1::UInt64, 9223372036854775806) AS n) EXCEPT ALL SELECT tuple(-1::Int64, -1::Int64) AS n);

-- Type names cannot see the flag: a flagged and an unflagged UInt64 print the same name. The number
-- of conversion steps can, because a branch whose column is already non constant reaches the common
-- header without a conversion step unless the predicate asks for one. These counts pin the
-- direction the predicate accepts. `legacy` keeps the step descriptions the count matches on, and
-- `optimize = 0` keeps the count independent of optimization passes.
SELECT 'conversion steps: primary';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT 10 AS n) EXCEPT ALL SELECT -9223372036854775807 AS n))
SETTINGS explain_query_plan_default = 'legacy';

SELECT 'conversion steps: mixed branches, flagged first';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT -1::Int64 AS n))
SETTINGS explain_query_plan_default = 'legacy';

SELECT 'conversion steps: mixed branches, unflagged first';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT number AS n FROM numbers(3)) EXCEPT ALL SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT -1::Int64 AS n))
SETTINGS explain_query_plan_default = 'legacy';

SELECT 'conversion steps: tuple with flags in both directions';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT tuple(9223372036854775806, 1::UInt64) AS n) EXCEPT ALL SELECT tuple(1::UInt64, 9223372036854775806) AS n) EXCEPT ALL SELECT tuple(-1::Int64, -1::Int64) AS n))
SETTINGS explain_query_plan_default = 'legacy';

SELECT 'conversion steps: union all';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT 9223372036854775806 AS n) UNION ALL SELECT 10 AS n) UNION ALL SELECT -9223372036854775807 AS n))
SETTINGS explain_query_plan_default = 'legacy';

SELECT 'conversion steps: explicit casts';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM (((SELECT 9223372036854775806::UInt64 AS n) EXCEPT ALL SELECT 10::UInt8 AS n) EXCEPT ALL SELECT -9223372036854775807::Int64 AS n))
SETTINGS explain_query_plan_default = 'legacy';

-- The common type of a two branch union whose branch types compare equal is the first branch type
-- verbatim, flag included, so the second branch here would have to ACQUIRE the flag to match it.
-- That is the direction the predicate refuses, so it gets no conversion step.
SELECT 'conversion steps: a branch that would have to acquire the flag';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM ((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT number AS n FROM numbers(3)))
SETTINGS explain_query_plan_default = 'legacy';
SELECT DISTINCT toTypeName(n) FROM ((SELECT 9223372036854775806 AS n) EXCEPT ALL SELECT number AS n FROM numbers(3));

-- Both branches are non constant tuples carrying the flag on opposite elements, so converting one
-- to the other would drop one element's flag and add the other's at the same time. A CAST converts
-- a whole column and cannot do that, so this container is left alone and gets no conversion step.
SELECT 'conversion steps: non constant tuple with flags in both directions';
SELECT countIf(explain ILIKE '%Conversion before UNION%') FROM (EXPLAIN optimize = 0
    SELECT n FROM ((SELECT tuple(9223372036854775806, materialize(1::UInt64)) AS n) EXCEPT ALL SELECT tuple(materialize(1::UInt64), 9223372036854775806) AS n))
SETTINGS explain_query_plan_default = 'legacy';
SELECT DISTINCT toTypeName(n) FROM ((SELECT tuple(9223372036854775806, materialize(1::UInt64)) AS n) EXCEPT ALL SELECT tuple(materialize(1::UInt64), 9223372036854775806) AS n);

-- A union over aggregating branches keeps its aggregate state types, including in a secondary
-- query where the branch headers carry AggregateFunction state instead of the final type.
-- `prefer_localhost_replica = 0` keeps the remote branches out of the local planning shortcut, so
-- the secondary query headers are really built.
SELECT 'control: aggregate state union';
DROP TABLE IF EXISTS t_04647;
CREATE TABLE t_04647 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04647 SELECT number FROM numbers(5);
SELECT sum(s) FROM (SELECT sum(a) AS s FROM remote('127.0.0.1', currentDatabase(), t_04647) UNION ALL SELECT count() AS s FROM remote('127.0.0.1', currentDatabase(), t_04647)) SETTINGS prefer_localhost_replica = 0;
SELECT DISTINCT toTypeName(s) FROM (SELECT sum(a) AS s FROM remote('127.0.0.1', currentDatabase(), t_04647) UNION ALL SELECT count() AS s FROM remote('127.0.0.1', currentDatabase(), t_04647)) SETTINGS prefer_localhost_replica = 0;
DROP TABLE t_04647;
