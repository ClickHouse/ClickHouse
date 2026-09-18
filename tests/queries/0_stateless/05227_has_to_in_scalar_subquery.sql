-- Tests that the has() -> IN rewrite reaches a constant array coming from a scalar subquery
-- (kept as a __getScalar call rather than folded into a literal) and a LowCardinality needle.

SET enable_analyzer = 1;
SET optimize_rewrite_has_to_in = 1;
SET explain_query_plan_default = 'legacy';

-- Rewritten shapes: the plan must contain in(), and the result must be the same as without the rewrite.

SELECT 'string', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'string plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'uint64', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(number) FROM numbers(5)), number);
SELECT 'uint64 plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(number) FROM numbers(5)), number)
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'int64 negative', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(-toInt64(number)) FROM numbers(5)), -toInt64(number));
SELECT 'int64 negative plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(-toInt64(number)) FROM numbers(5)), -toInt64(number))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'fixedstring', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFixedString(toString(number), 3)) FROM numbers(5)), toFixedString(toString(number), 3));
SELECT 'fixedstring plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFixedString(toString(number), 3)) FROM numbers(5)), toFixedString(toString(number), 3))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'date', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), toDate('2026-01-01') + number);
SELECT 'date plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), toDate('2026-01-01') + number)
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'decimal', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDecimal64(number, 3)) FROM numbers(5)), toDecimal64(number, 3));
SELECT 'decimal plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDecimal64(number, 3)) FROM numbers(5)), toDecimal64(number, 3))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'uuid', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0')))) FROM numbers(5)), toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))));
SELECT 'uuid plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0')))) FROM numbers(5)), toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'ipv4', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toIPv4(number + 1)) FROM numbers(5)), toIPv4(number + 1));
SELECT 'ipv4 plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toIPv4(number + 1)) FROM numbers(5)), toIPv4(number + 1))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'enum', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(CAST(if(number % 2, 'a', 'b'), 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')) FROM numbers(5)), CAST(['a', 'b', 'c'][(number % 3) + 1], 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'));
SELECT 'enum plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(CAST(if(number % 2, 'a', 'b'), 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')) FROM numbers(5)), CAST(['a', 'b', 'c'][(number % 3) + 1], 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'))
    ) WHERE explain LIKE '%FUNCTION in%';

-- A LowCardinality needle is rewritten too: the needle is cast to its dictionary type, so the
-- expression keeps returning UInt8 and parents resolved against UInt8 stay valid.
SELECT 'lowcardinality literal', count() FROM numbers(20) WHERE has(['1', '2', '3'], toLowCardinality(toString(number)));
SELECT 'lowcardinality literal plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has(['1', '2', '3'], toLowCardinality(toString(number)))
    ) WHERE explain LIKE '%FUNCTION in%';
SELECT 'lowcardinality literal type', toTypeName(has(['1', '2', '3'], toLowCardinality(toString(number)))) FROM numbers(1);
SELECT 'lowcardinality literal not', count() FROM numbers(20) WHERE NOT has(['1', '2', '3'], toLowCardinality(toString(number)));

SELECT 'lowcardinality scalar', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)));
SELECT 'lowcardinality scalar plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)))
    ) WHERE explain LIKE '%FUNCTION in%';

-- notHas goes through notIn, and NOT IN is renamed by transform_null_in.
SELECT 'nothas', count() FROM numbers(20) WHERE notHas((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'nothas plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE notHas((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%FUNCTION notIn%';

SET transform_null_in = 1;
SELECT 'transform_null_in 1', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'transform_null_in 1 plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%FUNCTION nullIn%';
SET transform_null_in = 0;

SET validate_enum_literals_in_operators = 1;
SELECT 'validate_enum_literals 1', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(CAST(if(number % 2, 'a', 'b'), 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')) FROM numbers(5)), CAST(['a', 'b', 'c'][(number % 3) + 1], 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'));
SET validate_enum_literals_in_operators = 0;

-- The set is built on the shard from the scalar it was sent, not on the initiator only.
SELECT 'remote', count() FROM remote('127.0.0.{1,2}', numbers(20)) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'remote lowcardinality', count() FROM remote('127.0.0.{1,2}', numbers(20)) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)));

-- Shapes that must NOT be rewritten, with results unchanged.

SELECT 'empty scalar array', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(0)), toString(number));
-- An `in` over an empty set folds to a constant, so the plan is asserted by the surviving has()
-- rather than by the absence of in(): only the first form can tell the two cases apart.
SELECT 'empty scalar array kept as has', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(0)), toString(number))
    ) WHERE explain LIKE '%Filter column: has(%';

SELECT 'null element scalar array', count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> if(x % 2, NULL, toString(x)), range(5))), toString(number));
SELECT 'null element scalar array plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> if(x % 2, NULL, toString(x)), range(5))), toString(number))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'nullable element scalar array', count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> toNullable(toString(x)), range(5))), toString(number));
SELECT 'nullable element scalar array plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> toNullable(toString(x)), range(5))), toString(number))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'float scalar array', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFloat64(number)) FROM numbers(5)), toFloat64(number));
SELECT 'float scalar array plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFloat64(number)) FROM numbers(5)), toFloat64(number))
    ) WHERE explain LIKE '%FUNCTION in%';
SELECT 'float minus zero', has((SELECT groupUniqArray(toFloat64(0.0)) FROM numbers(1)), materialize(toFloat64(-0.0)));

SELECT 'nullable needle', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toNullable(toString(number)));
SELECT 'nullable needle plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toNullable(toString(number)))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'mismatched types', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), materialize(toDateTime('2026-01-01 12:34:56')));
SELECT 'mismatched types plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), materialize(toDateTime('2026-01-01 12:34:56')))
    ) WHERE explain LIKE '%FUNCTION in%';

SELECT 'nested array scalar', count() FROM numbers(20) WHERE has((SELECT groupUniqArray([toString(number)]) FROM numbers(5)), [toString(number)]);
SELECT 'nested array scalar plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray([toString(number)]) FROM numbers(5)), [toString(number)])
    ) WHERE explain LIKE '%FUNCTION in%';

-- Same results with the rewrite switched off, which is what makes the rows above a value oracle
-- and not just a plan oracle.

SET optimize_rewrite_has_to_in = 0;

SELECT 'off string', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'off uint64', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(number) FROM numbers(5)), number);
SELECT 'off int64 negative', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(-toInt64(number)) FROM numbers(5)), -toInt64(number));
SELECT 'off fixedstring', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFixedString(toString(number), 3)) FROM numbers(5)), toFixedString(toString(number), 3));
SELECT 'off date', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), toDate('2026-01-01') + number);
SELECT 'off decimal', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDecimal64(number, 3)) FROM numbers(5)), toDecimal64(number, 3));
SELECT 'off uuid', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0')))) FROM numbers(5)), toUUID(concat('00000000-0000-0000-0000-', leftPad(toString(number), 12, '0'))));
SELECT 'off ipv4', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toIPv4(number + 1)) FROM numbers(5)), toIPv4(number + 1));
SELECT 'off enum', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(CAST(if(number % 2, 'a', 'b'), 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)')) FROM numbers(5)), CAST(['a', 'b', 'c'][(number % 3) + 1], 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'));
SELECT 'off lowcardinality literal', count() FROM numbers(20) WHERE has(['1', '2', '3'], toLowCardinality(toString(number)));
SELECT 'off lowcardinality literal type', toTypeName(has(['1', '2', '3'], toLowCardinality(toString(number)))) FROM numbers(1);
SELECT 'off lowcardinality literal not', count() FROM numbers(20) WHERE NOT has(['1', '2', '3'], toLowCardinality(toString(number)));
SELECT 'off lowcardinality scalar', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)));
SELECT 'off nothas', count() FROM numbers(20) WHERE notHas((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'off remote', count() FROM remote('127.0.0.{1,2}', numbers(20)) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number));
SELECT 'off remote lowcardinality', count() FROM remote('127.0.0.{1,2}', numbers(20)) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)));
SELECT 'off empty scalar array', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(0)), toString(number));
SELECT 'off null element scalar array', count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> if(x % 2, NULL, toString(x)), range(5))), toString(number));
SELECT 'off nullable element scalar array', count() FROM numbers(20) WHERE has((SELECT arrayMap(x -> toNullable(toString(x)), range(5))), toString(number));
SELECT 'off float scalar array', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toFloat64(number)) FROM numbers(5)), toFloat64(number));
SELECT 'off float minus zero', has((SELECT groupUniqArray(toFloat64(0.0)) FROM numbers(1)), materialize(toFloat64(-0.0)));
SELECT 'off nullable needle', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toNullable(toString(number)));
SELECT 'off mismatched types', count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toDate('2026-01-01') + number) FROM numbers(5)), materialize(toDateTime('2026-01-01 12:34:56')));
SELECT 'off nested array scalar', count() FROM numbers(20) WHERE has((SELECT groupUniqArray([toString(number)]) FROM numbers(5)), [toString(number)]);
SELECT 'off plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%FUNCTION in%';

SET optimize_rewrite_has_to_in = 1;

-- A primary key predicate still prunes granules after the rewrite, including for a
-- LowCardinality key, whose needle is wrapped into a cast.

DROP TABLE IF EXISTS tab_pk_lc;
CREATE TABLE tab_pk_lc (k LowCardinality(String), v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO tab_pk_lc SELECT toString(number), number FROM numbers(1000);

SELECT 'pk granules literal', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_pk_lc WHERE has(['1', '2', '3'], k)
    ) WHERE explain LIKE '%Granules:%';
SELECT 'pk granules scalar', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_pk_lc WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(4)), k)
    ) WHERE explain LIKE '%Granules:%';
SELECT 'pk result literal', count() FROM tab_pk_lc WHERE has(['1', '2', '3'], k);
SELECT 'pk result scalar', count() FROM tab_pk_lc WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(4)), k);

DROP TABLE tab_pk_lc;

-- A set skip index is what made the reported query slow: with the rewrite it is now analyzed as a
-- set membership test instead of a linear scan over the constant array.

DROP TABLE IF EXISTS tab_set_index;
CREATE TABLE tab_set_index (type UInt32, uid LowCardinality(String), INDEX idx_uid uid TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY type SETTINGS index_granularity = 64;
INSERT INTO tab_set_index SELECT 1, toString(number % 100) FROM numbers(10000);

SELECT 'skip index plan', count() FROM (
    EXPLAIN actions=1,header=1 WITH (SELECT groupUniqArray(uid) FROM tab_set_index) AS uniqs SELECT count(DISTINCT uid) FROM tab_set_index WHERE type = 1 AND uid IN (uniqs)
    ) WHERE explain LIKE '%FUNCTION in%';
WITH (SELECT groupUniqArray(uid) FROM tab_set_index) AS uniqs SELECT 'skip index result', count(DISTINCT uid) FROM tab_set_index WHERE type = 1 AND uid IN (uniqs);
WITH (SELECT groupUniqArray(uid) FROM tab_set_index) AS uniqs SELECT 'skip index result off', count(DISTINCT uid) FROM tab_set_index WHERE type = 1 AND uid IN (uniqs) SETTINGS optimize_rewrite_has_to_in = 0;

DROP TABLE tab_set_index;
