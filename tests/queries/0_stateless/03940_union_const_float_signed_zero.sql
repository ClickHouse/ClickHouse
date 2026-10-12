-- Regression test for #121888.
-- UNION ALL branches whose constants compare IEEE-equal but differ bitwise
-- (-0.0 vs 0.0, or two NaN payloads) must each keep their own value instead of
-- collapsing onto the first branch's constant.

SELECT hex(reinterpretAsUInt64(f)) FROM (SELECT 0.0 AS f UNION ALL SELECT -0.0 AS f) ORDER BY 1;
SELECT count() FROM (SELECT DISTINCT f FROM (SELECT 0.0 AS f UNION ALL SELECT -0.0 AS f));

-- Reference behaviour: the same query over materialized (non-const) columns.
SELECT hex(reinterpretAsUInt64(f)) FROM (SELECT materialize(0.0) AS f UNION ALL SELECT materialize(-0.0) AS f) ORDER BY 1;

-- Nested constants containing floats follow the same bit-exact rule.
SELECT hex(reinterpretAsUInt64(a[1])) FROM (SELECT [0.0] AS a UNION ALL SELECT [-0.0] AS a) ORDER BY 1;
SELECT hex(reinterpretAsUInt64(tupleElement(t, 1))) FROM (SELECT tuple(0.0) AS t UNION ALL SELECT tuple(-0.0) AS t) ORDER BY 1;
