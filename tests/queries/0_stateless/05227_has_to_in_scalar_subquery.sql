-- Tests that the has() -> IN rewrite reaches a constant array coming from a scalar subquery
-- (kept as a __getScalar call rather than folded into a literal) and a LowCardinality needle.

SET enable_analyzer = 1;
SET optimize_rewrite_has_to_in = 1;
SET enable_scalar_subquery_optimization = 1;
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

-- An Array(LowCardinality(T)) source is rewritten too: the element and needle types are compared with LowCardinality stripped.
SELECT 'lowcardinality array scalar', count() FROM numbers(20) WHERE has((SELECT [toLowCardinality('1'), toLowCardinality('2')]), toString(number));
SELECT 'lowcardinality array scalar plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has((SELECT [toLowCardinality('1'), toLowCardinality('2')]), toString(number))
    ) WHERE explain LIKE '%FUNCTION in%';
SELECT 'lowcardinality array literal', count() FROM numbers(20) WHERE has([toLowCardinality('1'), toLowCardinality('2')], toString(number));
SELECT 'lowcardinality array literal plan', count() FROM (
    EXPLAIN actions=1,header=1 SELECT count() FROM numbers(20) WHERE has([toLowCardinality('1'), toLowCardinality('2')], toString(number))
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

-- One scalar array shared by two needle types needs one Set per needle type: the set is built by
-- converting the array's elements to the needle's type, so each type keeps a different subset of
-- them. Both `in`s carry the same set source, so a set key that ignores the needle type would give
-- the second predicate the first one's set. The needles stay non-constant so that the predicates
-- survive into the actions DAG, and both orders are checked because only the elements the FIRST
-- key dropped can go missing.
-- 256 is representable as UInt16 but not as UInt8:
WITH (SELECT groupUniqArray(toUInt16(256)) FROM numbers(1)) AS a
SELECT 'set key per needle type', countIf(has(a, toUInt8(number))), countIf(has(a, toUInt16(number + 256))) FROM numbers(1);
WITH (SELECT groupUniqArray(toUInt16(256)) FROM numbers(1)) AS a
SELECT 'set key per needle type reversed', countIf(has(a, toUInt16(number + 256))), countIf(has(a, toUInt8(number))) FROM numbers(1);
-- Neither Int8 nor UInt8 can hold both -1 and 200, so here each type drops the element the other
-- one keeps and a shared set loses a row in either order.
WITH (SELECT groupUniqArray(x) FROM (SELECT CAST(arrayJoin([-1, 200]), 'Int16') AS x)) AS a
SELECT 'set key per needle type disjoint', countIf(has(a, toInt8(number - 1))), countIf(has(a, toUInt8(number + 200))) FROM numbers(1);
WITH (SELECT groupUniqArray(x) FROM (SELECT CAST(arrayJoin([-1, 200]), 'Int16') AS x)) AS a
SELECT 'set key per needle type disjoint reversed', countIf(has(a, toUInt8(number + 200))), countIf(has(a, toInt8(number - 1))) FROM numbers(1);

-- The rows above are all satisfied by a rewrite of a folded literal array, so on their own they do
-- not say that the array reached the pass as a scalar subquery. These count the nodes of the
-- post-pass tree instead: a rewritten scalar shape keeps the __getScalar call as the set source
-- beside the in(). The `folded` rows are the negative control that makes the other rows
-- discriminating: with the scalar folded to a literal the rewrite still happens, through the
-- pre-existing branch, so in() is still there and __getScalar must be gone.

SELECT 'scalar shape getscalar', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%__getScalar%';
SELECT 'scalar shape in', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    ) WHERE explain LIKE '%function_name: in,%';
SELECT 'scalar shape getscalar folded', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    SETTINGS enable_scalar_subquery_optimization = 0
    ) WHERE explain LIKE '%__getScalar%';
SELECT 'scalar shape in folded', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toString(number))
    SETTINGS enable_scalar_subquery_optimization = 0
    ) WHERE explain LIKE '%function_name: in,%';

SELECT 'lowcardinality scalar shape getscalar', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)))
    ) WHERE explain LIKE '%__getScalar%';
SELECT 'lowcardinality scalar shape in', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)))
    ) WHERE explain LIKE '%function_name: in,%';
SELECT 'lowcardinality scalar shape getscalar folded', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)))
    SETTINGS enable_scalar_subquery_optimization = 0
    ) WHERE explain LIKE '%__getScalar%';
SELECT 'lowcardinality scalar shape in folded', count() FROM (
    EXPLAIN QUERY TREE run_passes = 1 SELECT count() FROM numbers(20)
    WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(5)), toLowCardinality(toString(number)))
    SETTINGS enable_scalar_subquery_optimization = 0
    ) WHERE explain LIKE '%function_name: in,%';

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
WITH (SELECT groupUniqArray(toUInt16(256)) FROM numbers(1)) AS a
SELECT 'off set key per needle type', countIf(has(a, toUInt8(number))), countIf(has(a, toUInt16(number + 256))) FROM numbers(1);
WITH (SELECT groupUniqArray(toUInt16(256)) FROM numbers(1)) AS a
SELECT 'off set key per needle type reversed', countIf(has(a, toUInt16(number + 256))), countIf(has(a, toUInt8(number))) FROM numbers(1);
WITH (SELECT groupUniqArray(x) FROM (SELECT CAST(arrayJoin([-1, 200]), 'Int16') AS x)) AS a
SELECT 'off set key per needle type disjoint', countIf(has(a, toInt8(number - 1))), countIf(has(a, toUInt8(number + 200))) FROM numbers(1);
WITH (SELECT groupUniqArray(x) FROM (SELECT CAST(arrayJoin([-1, 200]), 'Int16') AS x)) AS a
SELECT 'off set key per needle type disjoint reversed', countIf(has(a, toUInt8(number + 200))), countIf(has(a, toInt8(number - 1))) FROM numbers(1);
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

-- The same four rows without the rewrite. They must match the rows above, which makes those an
-- equivalence oracle instead of a snapshot; at the default setting the has() atom over a
-- LowCardinality key is not reached at all (03402 covers it with the rewrite off too).
SELECT 'pk granules literal off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_pk_lc WHERE has(['1', '2', '3'], k) SETTINGS optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%';
SELECT 'pk granules scalar off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_pk_lc WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(4)), k) SETTINGS optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%';
SELECT 'pk result literal off', count() FROM tab_pk_lc WHERE has(['1', '2', '3'], k) SETTINGS optimize_rewrite_has_to_in = 0;
SELECT 'pk result scalar off', count() FROM tab_pk_lc WHERE has((SELECT groupUniqArray(toString(number)) FROM numbers(4)), k) SETTINGS optimize_rewrite_has_to_in = 0;

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

-- The fixture above is the reported query, whose needle set covers every stored value, so it can
-- only show the index being cheap and never show it pruning. This one is selective: each granule
-- holds a single uid and the needle names two of them, so the index has to drop granules, and it
-- has to drop the same ones with and without the rewrite. `index_granularity_bytes = 0` keeps the
-- granule size at the row count the assertions below are written for; parts have to be Wide for
-- that, so the two wide-part thresholds go with it.

DROP TABLE IF EXISTS tab_set_index_selective;
CREATE TABLE tab_set_index_selective (id UInt32, uid LowCardinality(String), INDEX idx_uid uid TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab_set_index_selective SELECT number, toString(intDiv(number, 64)) FROM numbers(640);

-- The primary key is on `id` and every predicate below is on `uid` alone, so the `Granules:` line of
-- the PrimaryKey block stays at the full count and only the skip index prunes. The rows are
-- `Granules: <primary key>`, `Name: idx_uid`, `Granules: <skip index>`, in that order.
SELECT 'selective skip index', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_set_index_selective
    WHERE uid IN ((SELECT groupUniqArray(uid) FROM tab_set_index_selective WHERE id < 128))
    SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'selective skip index off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_set_index_selective
    WHERE uid IN ((SELECT groupUniqArray(uid) FROM tab_set_index_selective WHERE id < 128))
    SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'selective result', count() FROM tab_set_index_selective
    WHERE uid IN ((SELECT groupUniqArray(uid) FROM tab_set_index_selective WHERE id < 128)) SETTINGS use_skip_indexes = 1;
SELECT 'selective result off', count() FROM tab_set_index_selective
    WHERE uid IN ((SELECT groupUniqArray(uid) FROM tab_set_index_selective WHERE id < 128)) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'selective result no skip index', count() FROM tab_set_index_selective
    WHERE uid IN ((SELECT groupUniqArray(uid) FROM tab_set_index_selective WHERE id < 128)) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_set_index_selective;

-- A hash-based skip index finds its column by the atom's rendered name, so the cast that wraps a
-- LowCardinality needle has to be matched too. For each index family below the granules dropped and the
-- acceptance by `force_data_skipping_indices` have to be the same with the rewrite on and off, and the
-- counts have to match the unindexed read. Fixture as above: each granule holds a single uid and the
-- needle names two of them. `ngrambf_v1` cannot discriminate 1-character values, so it prunes nothing in
-- either arm; the rows below still pin that it is consulted at all.

DROP TABLE IF EXISTS tab_bloom_lc;
DROP TABLE IF EXISTS tab_tokenbf_lc;
DROP TABLE IF EXISTS tab_ngrambf_lc;
CREATE TABLE tab_bloom_lc (id UInt32, uid LowCardinality(String), INDEX idx_uid uid TYPE bloom_filter(0.01) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE tab_tokenbf_lc (id UInt32, uid LowCardinality(String), INDEX idx_uid uid TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE tab_ngrambf_lc (id UInt32, uid LowCardinality(String), INDEX idx_uid uid TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO tab_bloom_lc SELECT number, toString(intDiv(number, 64)) FROM numbers(640);
INSERT INTO tab_tokenbf_lc SELECT number, toString(intDiv(number, 64)) FROM numbers(640);
INSERT INTO tab_ngrambf_lc SELECT number, toString(intDiv(number, 64)) FROM numbers(640);

SELECT 'bloom granules', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'bloom granules off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'bloom granules scalar', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_bloom_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'bloom granules scalar off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_bloom_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'bloom forced', count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid';
SELECT 'bloom forced off', count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid', optimize_rewrite_has_to_in = 0;
SELECT 'bloom result', count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1;
SELECT 'bloom result off', count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'bloom result no skip index', count() FROM tab_bloom_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 0;
SELECT 'bloom result scalar', count() FROM tab_bloom_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_bloom_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1;
SELECT 'bloom result scalar off', count() FROM tab_bloom_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_bloom_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'bloom result scalar no skip index', count() FROM tab_bloom_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_bloom_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 0;

SELECT 'tokenbf granules', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'tokenbf granules off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'tokenbf granules scalar', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_tokenbf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_tokenbf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'tokenbf granules scalar off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_tokenbf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_tokenbf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'tokenbf forced', count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid';
SELECT 'tokenbf forced off', count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid', optimize_rewrite_has_to_in = 0;
SELECT 'tokenbf result', count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1;
SELECT 'tokenbf result off', count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'tokenbf result no skip index', count() FROM tab_tokenbf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 0;
SELECT 'tokenbf result scalar', count() FROM tab_tokenbf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_tokenbf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1;
SELECT 'tokenbf result scalar off', count() FROM tab_tokenbf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_tokenbf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'tokenbf result scalar no skip index', count() FROM tab_tokenbf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_tokenbf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 0;

SELECT 'ngrambf granules', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'ngrambf granules off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'ngrambf granules scalar', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_ngrambf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_ngrambf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'ngrambf granules scalar off', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_ngrambf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_ngrambf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'ngrambf forced', count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid';
SELECT 'ngrambf forced off', count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS force_data_skipping_indices = 'idx_uid', optimize_rewrite_has_to_in = 0;
SELECT 'ngrambf result', count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1;
SELECT 'ngrambf result off', count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'ngrambf result no skip index', count() FROM tab_ngrambf_lc WHERE has(['1', '2'], uid) SETTINGS use_skip_indexes = 0;
SELECT 'ngrambf result scalar', count() FROM tab_ngrambf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_ngrambf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1;
SELECT 'ngrambf result scalar off', count() FROM tab_ngrambf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_ngrambf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 1, optimize_rewrite_has_to_in = 0;
SELECT 'ngrambf result scalar no skip index', count() FROM tab_ngrambf_lc WHERE has((SELECT groupUniqArray(uid) FROM tab_ngrambf_lc WHERE id < 128), uid) SETTINGS use_skip_indexes = 0;

-- Only a cast that does nothing but remove `LowCardinality` may reach the index, because only then does
-- the argument hold the bytes the index hashed. The three below are written directly, since the rewrite
-- emits no other cast shape: a narrower target type, a target type that keeps `LowCardinality`, and a
-- value-preserving conversion that is not a cast at all. None of them may be answered from the index.
SELECT 'narrow cast removing lowcardinality', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE CAST(uid, 'String') IN ('1', '2') SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'narrow cast to fixedstring', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE CAST(uid, 'FixedString(4)') IN ('1', '2') SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'narrow cast keeping lowcardinality', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE CAST(uid, 'LowCardinality(FixedString(4))') IN ('1', '2') SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';
SELECT 'narrow tostring', trimLeft(explain) FROM (
    EXPLAIN indexes=1 SELECT count() FROM tab_bloom_lc WHERE toString(uid) IN ('1', '2') SETTINGS use_skip_indexes = 1
    ) WHERE explain LIKE '%Granules:%' OR explain LIKE '%Name:%';

DROP TABLE tab_bloom_lc;
DROP TABLE tab_tokenbf_lc;
DROP TABLE tab_ngrambf_lc;
