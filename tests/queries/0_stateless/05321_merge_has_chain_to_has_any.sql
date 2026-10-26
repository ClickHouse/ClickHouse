-- `NOT has(arr, x) AND NOT has(arr, y)` is `NOT hasAny(arr, [x, y])`, and `has(arr, x) OR has(arr, y)` is `hasAny(arr, [x, y])`.
-- A text index answers one `hasAny` once per block, a chain of single-element calls once per call.
SET enable_analyzer = 1;
SET optimize_rewrite_has_chain_to_has_any = 1;
SET optimize_min_has_chain_length = 2;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (id UInt32, words Array(String), other Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab VALUES (1, ['just', 'think'], ['a']), (2, ['from'], ['just']), (3, ['people', 'your'], []), (4, [], ['b']), (5, ['just', 'from'], ['c']);

-- The chain and the merged form return the same rows.
SELECT id FROM tab WHERE NOT hasAll(words, ['just']) AND NOT has(words, 'from') AND NOT hasAll(words, ['people']) ORDER BY id;
SELECT id FROM tab WHERE NOT hasAny(words, ['just', 'from', 'people']) ORDER BY id;
SELECT id FROM tab WHERE has(words, 'just') OR hasAll(words, ['people']) ORDER BY id;
SELECT id FROM tab WHERE id > 1 AND notHas(words, 'just') AND NOT has(words, 'from') ORDER BY id;

-- The tree holds one `hasAny` and no `has` or `hasAll`; other operands of the chain stay.
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%' OR explain LIKE '%function_name: hasAll,%')
    FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAll(words, ['just']) AND NOT has(words, 'from') AND NOT hasAll(words, ['people']));
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%' OR explain LIKE '%function_name: hasAll,%')
    FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE has(words, 'just') OR hasAll(words, ['people']));
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: greater,%')
    FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE id > 1 AND notHas(words, 'just') AND NOT has(words, 'from'));

-- NULL elements in the array and a `LowCardinality` element type are compared the same way by `has` and `hasAny`.
DROP TABLE IF EXISTS tab_nullable;
CREATE TABLE tab_nullable (id UInt32, words Array(Nullable(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_nullable VALUES (1, ['just', NULL]), (2, [NULL]), (3, ['from']), (4, []);
SELECT id FROM tab_nullable WHERE NOT has(words, 'just') AND NOT hasAll(words, ['from']) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_nullable WHERE NOT has(words, 'just') AND NOT hasAll(words, ['from']));
DROP TABLE tab_nullable;
DROP TABLE IF EXISTS tab_lc;
CREATE TABLE tab_lc (id UInt32, words Array(LowCardinality(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_lc VALUES (1, ['just']), (2, ['from', 'x']), (3, ['y']);
SELECT id FROM tab_lc WHERE has(words, 'just') OR hasAll(words, ['from']) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_lc WHERE has(words, 'just') OR hasAll(words, ['from']));
DROP TABLE tab_lc;

-- A chain in the select list is left as written, and column names are kept.
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AS f, NOT has(words, 'just') AND NOT has(words, 'from') FROM tab WHERE id = 1 FORMAT TSVWithNames;

-- A positive call in an `AND` chain and a `Nullable` operand in an `OR` chain are left in place; the result type of the chain does not change.
SELECT id FROM tab WHERE has(words, 'think') AND NOT has(words, 'from') AND NOT has(words, 'people') ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE has(words, 'think') AND NOT has(words, 'from') AND NOT has(words, 'people'));
SELECT count() FROM tab WHERE has(words, 'just') OR has(words, 'from') OR toNullable(id = 1);
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: or, function_type: ordinary, result_type: Nullable(UInt8)%') FROM (EXPLAIN QUERY TREE SELECT count() FROM tab WHERE has(words, 'just') OR has(words, 'from') OR toNullable(id = 1));

-- The merge is done in WHERE, PREWHERE and JOIN ON, including a subquery's WHERE. A chain in the select list, GROUP BY, HAVING
-- or ORDER BY is left as written: its members may be grouping keys, which have to stay as written to be found after the aggregation.
-- An alias hands one node to several clauses; `WHERE k` is rewritten while `k` in the select list and the GROUP BY is not.
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AS k, count() FROM tab WHERE k GROUP BY k;
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AS k, count() FROM tab GROUP BY k HAVING k;
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AND id > 0 AS k, count() FROM tab WHERE k GROUP BY k;
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AND id > 0 AS k, count() FROM tab WHERE k GROUP BY k HAVING k;
-- The same when the chain sits inside a larger expression that the alias shares, and the GROUP BY keys are its members.
SELECT NOT (has(words, 'just') OR has(words, 'from')) AS k, count() FROM tab WHERE k GROUP BY has(words, 'just'), has(words, 'from');
SELECT NOT (has(words, 'just') OR has(words, 'from')) AS k, count() FROM tab WHERE k GROUP BY has(words, 'just'), has(words, 'from') HAVING k;
SELECT (NOT has(words, 'just') AND NOT has(words, 'from')) = 1 AS k, count() FROM tab WHERE k GROUP BY (NOT has(words, 'just') AND NOT has(words, 'from')) = 1;
SELECT id > 1 AND NOT (has(words, 'just') OR has(words, 'from')) AS k, count() FROM tab WHERE k GROUP BY id > 1, has(words, 'just'), has(words, 'from') ORDER BY count();
SELECT has(words, 'just') OR has(words, 'from') AS k, count() FROM tab GROUP BY has(words, 'just'), has(words, 'from') ORDER BY k, count();
SELECT NOT has(words, 'just') AND NOT has(words, 'from') AND NOT has(words, 'people') AND NOT has(words, 'your') AS k, count() FROM tab GROUP BY ROLLUP(k) ORDER BY k SETTINGS group_by_use_nulls = 1;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT has(words, 'just') OR has(words, 'from') AS k, count() FROM tab GROUP BY has(words, 'just'), has(words, 'from') HAVING has(words, 'just') OR has(words, 'from') ORDER BY k);
SELECT count() FROM tab PREWHERE NOT has(words, 'just') AND NOT has(words, 'from') WHERE NOT has(other, 'a') AND NOT has(other, 'b');
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT count() FROM tab PREWHERE NOT has(words, 'just') AND NOT has(words, 'from') WHERE NOT has(other, 'a') AND NOT has(other, 'b'));
SELECT count() FROM tab AS l INNER JOIN tab AS r ON l.id = r.id AND NOT has(l.words, 'just') AND NOT has(l.words, 'from');
SELECT count() FROM tab AS l LEFT JOIN tab AS r ON l.id = r.id AND NOT has(l.words, 'just') AND NOT has(l.words, 'from');
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT count() FROM tab AS l INNER JOIN tab AS r ON l.id = r.id AND NOT has(l.words, 'just') AND NOT has(l.words, 'from'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from')));
SELECT (SELECT count() FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from')) AS c FROM tab LIMIT 1;
-- A subquery outside the filters keeps its own `WHERE` rewritten (a scalar subquery is folded before the passes run, so it is not visible here).
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id IN (SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from')) FROM tab);
-- A subquery inside a rewritten filter, and a chain inside a lambda, are rewritten on the copy as well.
SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND id IN (SELECT id FROM tab WHERE NOT has(other, 'a') AND NOT has(other, 'b')) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND id IN (SELECT id FROM tab WHERE NOT has(other, 'a') AND NOT has(other, 'b')));
DROP TABLE IF EXISTS tab_nested;
CREATE TABLE tab_nested (id UInt32, nested Array(Array(String))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_nested VALUES (1, [['a', 'x']]), (2, [['b']]), (3, [['c', 'd']]), (4, []);
SELECT id FROM tab_nested WHERE arrayExists(x -> NOT has(x, 'a') AND NOT has(x, 'b'), nested) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_nested WHERE arrayExists(x -> NOT has(x, 'a') AND NOT has(x, 'b'), nested));
DROP TABLE tab_nested;

-- `tokens` of a constant is folded to a constant array, so the issue's spelling merges too; a two-word title stays as a two-element `hasAll`.
SELECT id FROM tab WHERE NOT hasAll(words, tokens('just')) AND NOT hasAll(words, tokens('from')) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: hasAll,%')
    FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAll(words, tokens('just')) AND NOT hasAll(words, tokens('from')) AND NOT hasAll(words, tokens('just think')));

-- An integer constant of another width is taken when it converts to the element type and back without change.
DROP TABLE IF EXISTS tab_ids;
CREATE TABLE tab_ids (id UInt32, ids Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_ids VALUES (1, [5, 6]), (2, [7]), (3, [8]);
SELECT id FROM tab_ids WHERE NOT has(ids, 5) AND NOT has(ids, 7) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_ids WHERE NOT has(ids, 5) AND NOT has(ids, 7));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_ids WHERE NOT has(ids, -1) AND NOT has(ids, 7));

-- Constants of a different kind than the element type are not merged: `String` for `Enum` or `FixedString`, and a
-- `Float64` that does not survive the round trip through `Float32`.
DROP TABLE IF EXISTS tab_types;
CREATE TABLE tab_types (id UInt32, e Array(Enum8('a' = 1, 'b' = 2)), f Array(FixedString(3)), x Array(Float32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_types VALUES (1, ['a'], ['abc'], [1.5]);
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_types WHERE NOT has(e, 'a') AND NOT has(e, 'b'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_types WHERE NOT has(f, 'abc') AND NOT has(f, 'xyz'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_types WHERE NOT has(x, 0.1) AND NOT has(x, 0.2));
DROP TABLE tab_types;

-- Calls on different arrays, on a non-deterministic array, a `hasAll` with two elements, a NULL element, a single call and a
-- disabled setting are left alone. A member already written as `hasAny` joins the chain.
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(other, 'just'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE has([rand() % 2], toUInt32(1)) OR has([rand() % 2], toUInt32(0)));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAll(words, ['just', 'think']) AND NOT has(words, 'from'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, NULL) AND NOT has(words, 'from'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') SETTINGS optimize_rewrite_has_chain_to_has_any = 0);
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAny(words, ['just', 'from']) AND NOT has(words, 'people'));

-- By default a chain needs four calls on the same array; the count is per array.
SET optimize_min_has_chain_length = DEFAULT;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND NOT has(words, 'people'));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND NOT has(words, 'people') AND NOT has(words, 'your'));
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%')
    FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND NOT has(other, 'a') AND NOT has(other, 'b') AND NOT has(other, 'c') AND NOT has(other, 'd'));
SELECT id FROM tab WHERE NOT has(words, 'just') AND NOT has(words, 'from') AND NOT has(other, 'a') AND NOT has(other, 'b') AND NOT has(other, 'c') AND NOT has(other, 'd') ORDER BY id;

-- Needles are sets, so `hasAny` members merge by union.
SET optimize_min_has_chain_length = 2;
SELECT 'union of hasAny members';
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: hasAll,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE hasAny(words, ['just']) OR has(words, 'from') OR hasAny(words, ['people', 'your']));
SELECT id FROM tab WHERE hasAny(words, ['just']) OR has(words, 'from') OR hasAny(words, ['people', 'your']) ORDER BY id;
SELECT id FROM tab WHERE hasAny(words, ['just']) OR has(words, 'from') OR hasAny(words, ['people', 'your']) ORDER BY id SETTINGS optimize_rewrite_has_chain_to_has_any = 0;
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: not,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAny(words, ['just', 'from']) AND NOT hasAny(words, ['people']) AND notHas(words, 'your'));
SELECT id FROM tab WHERE NOT hasAny(words, ['just', 'from']) AND NOT hasAny(words, ['people']) AND notHas(words, 'your') ORDER BY id;
SELECT id FROM tab WHERE NOT hasAny(words, ['just', 'from']) AND NOT hasAny(words, ['people']) AND notHas(words, 'your') ORDER BY id SETTINGS optimize_rewrite_has_chain_to_has_any = 0;

SELECT 'no merged form: a chain of plain has or hasAll under AND, NOT hasAll under AND, hasAny under AND; an empty or mistyped member is kept and the rest still merge';
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: hasAll,%'), countIf(explain LIKE '%function_name: has,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE has(words, 'just') AND hasAll(words, ['from']) AND has(words, 'people'));
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: hasAll,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE NOT hasAll(words, ['just', 'from']) AND NOT hasAll(words, ['people', 'your']));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE hasAny(words, ['just', 'from']) AND hasAny(words, ['people', 'your']));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE hasAny(words, []) OR has(words, 'just') OR hasAny(words, ['from']));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_ids WHERE hasAny(ids, [1, -1]) OR has(ids, 2) OR has(ids, 3));
-- A `hasAny` whose constant has no common type with the array throws on its own, so it is kept and keeps throwing.
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_ids WHERE hasAny(ids, [5.0, 7.0]) OR has(ids, 6) OR has(ids, 8));
SELECT id FROM tab_ids WHERE hasAny(ids, [5.0, 7.0]) OR has(ids, 6) OR has(ids, 8); -- { serverError NO_COMMON_TYPE }
-- A `hasAny` constant of another width that does convert merges, a NULL needle inside one keeps the member whole, and a duplicate needle is harmless.
SELECT id FROM tab_ids WHERE hasAny(ids, [5, 7]) OR has(ids, 6) ORDER BY id;
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_ids WHERE hasAny(ids, [5, 7]) OR has(ids, 6));
SELECT countIf(explain LIKE '%function_name: hasAny,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab WHERE hasAny(words, ['from', NULL]) OR has(words, 'just') OR has(words, 'people'));
SELECT id FROM tab WHERE has(words, 'just') OR hasAny(words, ['just', 'from']) ORDER BY id;

-- Float needles merge when they convert exactly. A NaN needle does not: `has` finds it in a `LowCardinality` dictionary
-- by its bytes and `hasAny` compares values, so the member is kept and the rows stay the same with the rewrite on and off.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS tab_floats;
CREATE TABLE tab_floats (id UInt32, f Array(Float64), lc Array(LowCardinality(Float64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO tab_floats VALUES (1, [nan], [nan]), (2, [1.0], [1.0]), (3, [0.5, 7.0], [0.5, 7.0]), (4, [], []);
SELECT 'floats';
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_floats WHERE has(f, 0.5) OR has(f, 1) OR has(f, 2) OR has(f, 3));
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE has(f, 0.5) OR has(f, 1) OR has(f, 2) OR has(f, 3) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE has(f, 0.5) OR has(f, 1) OR has(f, 2) OR has(f, 3) ORDER BY id) SETTINGS optimize_rewrite_has_chain_to_has_any = 0;
SELECT countIf(explain LIKE '%function_name: hasAny,%'), countIf(explain LIKE '%function_name: has,%') FROM (EXPLAIN QUERY TREE SELECT id FROM tab_floats WHERE has(lc, nan) OR has(lc, 1.0) OR has(lc, 2.0) OR has(lc, 3.0));
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE has(lc, nan) OR has(lc, 1.0) OR has(lc, 2.0) OR has(lc, 3.0) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE has(lc, nan) OR has(lc, 1.0) OR has(lc, 2.0) OR has(lc, 3.0) ORDER BY id) SETTINGS optimize_rewrite_has_chain_to_has_any = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE NOT has(lc, nan) AND NOT has(lc, 1.0) AND NOT has(lc, 2.0) AND NOT has(lc, 3.0) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_floats WHERE NOT has(lc, nan) AND NOT has(lc, 1.0) AND NOT has(lc, 2.0) AND NOT has(lc, 3.0) ORDER BY id) SETTINGS optimize_rewrite_has_chain_to_has_any = 0;
DROP TABLE tab_floats;
SET optimize_min_has_chain_length = DEFAULT;

DROP TABLE tab;
DROP TABLE tab_ids;
