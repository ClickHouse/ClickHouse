-- A JOIN key that contains an expression already computed by the filter or the subquery directly below the JOIN
-- reads that column instead of computing the expression again. Each case counts how often the shared function is
-- computed in the plan, and compares the result with `query_plan_merge_expressions = 0`, which turns this off.
-- The cases marked "not reused" check that it is not done when it would change the result: a function computed only
-- for some rows, a function that must run once per use, a function that captured different settings, and a function
-- whose result depends on the name of its argument.

SET enable_analyzer = 1;
SET query_plan_merge_expressions = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_merge_filters = 1;
SET query_plan_convert_outer_join_to_inner_join = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;
SET compile_expressions = 0;
SET date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 0;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS lg;
DROP TABLE IF EXISTS rg;
DROP TABLE IF EXISTS rg2;
DROP TABLE IF EXISTS ln;
DROP TABLE IF EXISTS rn;
DROP TABLE IF EXISTS lm;
DROP TABLE IF EXISTS ldt;
DROP TABLE IF EXISTS rdt;
DROP TABLE IF EXISTS lz;
DROP TABLE IF EXISTS rz;

CREATE TABLE l (id UInt64, k UInt64, s String, d DateTime64(6, 'UTC'), j JSON) ENGINE = MergeTree ORDER BY id;
CREATE TABLE r (id UInt64, v String) ENGINE = MergeTree ORDER BY id;

INSERT INTO l SELECT number, number % 7, '{"a":"' || toString(number % 13) || '","b":"' || toString(number) || '"}',
    toDateTime64('2026-01-01 00:00:00.120', 6, 'UTC') + toIntervalSecond(number),
    '{"a":"x' || toString(number % 13) || '","b":"y' || toString(number) || '"}'
FROM numbers(1000);

INSERT INTO r SELECT number, toString(number) FROM numbers(13);
INSERT INTO r SELECT 100 + number, toString(number) || '_' FROM numbers(7);
INSERT INTO r SELECT 200 + id, toString(d) FROM l WHERE id < 20;
INSERT INTO r SELECT 300 + id, concat('{"s":"', replaceAll(s, '"', '\\"'), '"}') FROM l WHERE id < 20;
INSERT INTO r SELECT 400 + id, lower(reverse(s)) FROM l WHERE id < 20;
INSERT INTO r SELECT 500 + number, 'y' || toString(number) FROM numbers(13);

CREATE TABLE lg (id UInt64, ok Bool, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO lg VALUES (1, false, 'abc'), (2, true, '5'), (3, true, '7');
CREATE TABLE rg (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO rg VALUES (5), (7), (9);
CREATE TABLE rg2 (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO rg2 VALUES (25), (37), (38);
CREATE TABLE lm (id UInt64, s String) ENGINE = Memory;
INSERT INTO lm VALUES (1, 'abc'), (2, '5'), (3, '7');
CREATE TABLE ldt (id UInt64, dd Date) ENGINE = MergeTree ORDER BY id;
INSERT INTO ldt VALUES (1, '2106-02-07'), (2, '2026-01-01');
CREATE TABLE rdt (t DateTime) ENGINE = MergeTree ORDER BY t;
INSERT INTO rdt VALUES (4294967295), ('2026-01-01 23:59:59');
CREATE TABLE lz (id UInt64, x Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lz VALUES (1, -0.), (2, 0.), (3, 1.5);
CREATE TABLE rz (v String) ENGINE = MergeTree ORDER BY v;
INSERT INTO rz VALUES ('0'), ('1.5');

CREATE TABLE ln (id UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO ln SELECT number, if(number % 5 = 0, NULL, number % 7) FROM numbers(100);
CREATE TABLE rn (id UInt64, vn Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO rn SELECT number, toString(number) || '_' FROM numbers(7);
INSERT INTO rn VALUES (100, NULL);

SELECT '1. part of the key is computed by the filter below the join';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != '')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '2. the whole key is computed by the filter below the join';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON toString(l.d) = r.v WHERE toString(l.d) != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON toString(l.d) = r.v WHERE toString(l.d) != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON toString(l.d) = r.v WHERE toString(l.d) != '')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '3. the key on the right side of the join';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM r JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM r JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM r JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '4. outer joins';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM r RIGHT JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM r RIGHT JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM r RIGHT JOIN l ON r.v = concat(toString(l.k), '_') WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l FULL JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '0');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l FULL JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '0');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l FULL JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '0')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3' SETTINGS join_use_nulls = 1);
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3')
    SETTINGS join_use_nulls = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l LEFT JOIN r ON concat(toString(l.k), '_') = r.v WHERE toString(l.k) != '3')
    SETTINGS join_use_nulls = 1, query_plan_merge_expressions = 0;

SELECT '5. the expression is computed by a subquery below the join';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v)
    SETTINGS query_plan_merge_expressions = 0;

SELECT '6. not reused: the subquery computes the expression only for some rows';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, r.id FROM (SELECT id, k, if(id % 2 = 0, toString(k), '') AS y FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.y, r.id FROM (SELECT id, k, if(id % 2 = 0, toString(k), '') AS y FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.y, r.id FROM (SELECT id, k, if(id % 2 = 0, toString(k), '') AS y FROM l) AS a JOIN r ON concat(toString(a.k), '_') = r.v)
    SETTINGS query_plan_merge_expressions = 0;

SELECT '7. not reused: the expression contains a function that must be computed for each use';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON concat(toString(plus(sleepEachRow(0), l.k)), '_') = r.v WHERE toString(plus(sleepEachRow(0), l.k)) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON concat(toString(plus(sleepEachRow(0), l.k)), '_') = r.v WHERE toString(plus(sleepEachRow(0), l.k)) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON concat(toString(plus(sleepEachRow(0), l.k)), '_') = r.v WHERE toString(plus(sleepEachRow(0), l.k)) != '3')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '8. not reused: the subquery computes the expression with different settings';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ds, r.id FROM (SELECT id, d, toString(d) AS ds FROM l SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1) AS a
    JOIN r ON concat(toString(a.d), '') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ds, r.id FROM (SELECT id, d, toString(d) AS ds FROM l SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1) AS a
    JOIN r ON concat(toString(a.d), '') = r.v);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ds, r.id FROM (SELECT id, d, toString(d) AS ds FROM l SETTINGS date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands = 1) AS a
    JOIN r ON concat(toString(a.d), '') = r.v) SETTINGS query_plan_merge_expressions = 0;

SELECT '9. the filter is also pushed to the other side of the join';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON toString(l.k) = r.v WHERE toString(l.k) != '3');
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON toString(l.k) = r.v WHERE toString(l.k) != '3' SETTINGS query_plan_merge_expressions = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON toString(l.k) = r.v WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON toString(l.k) = r.v WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '10. a join over the rewritten join';
SELECT countIf(explain ILIKE '%FUNCTION less(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3');
SELECT countIf(explain ILIKE '%FUNCTION less(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3'
    SETTINGS query_plan_merge_expressions = 0);
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3');
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3'
    SETTINGS query_plan_merge_expressions = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id, r3.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v JOIN r AS r3 ON l.id = r3.id WHERE l.id < 10 AND toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '11. not reused: the function depends on the name of its argument';
SELECT countIf(explain ILIKE '%FUNCTION formatRowNoNewline(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.f, r.id FROM (SELECT id, s, formatRowNoNewline('JSONEachRow', s AS x) AS f FROM l) AS a
    JOIN r ON formatRowNoNewline('JSONEachRow', a.s) = r.v WHERE a.f != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.f, r.id FROM (SELECT id, s, formatRowNoNewline('JSONEachRow', s AS x) AS f FROM l) AS a
    JOIN r ON formatRowNoNewline('JSONEachRow', a.s) = r.v WHERE a.f != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.f, r.id FROM (SELECT id, s, formatRowNoNewline('JSONEachRow', s AS x) AS f FROM l) AS a
    JOIN r ON formatRowNoNewline('JSONEachRow', a.s) = r.v WHERE a.f != '') SETTINGS query_plan_merge_expressions = 0;

SELECT '12. not reused: a function that does not declare that it ignores argument names';
SELECT countIf(explain ILIKE '%FUNCTION reverse(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON lower(reverse(l.s)) = r.v WHERE reverse(l.s) != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON lower(reverse(l.s)) = r.v WHERE reverse(l.s) != '');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON lower(reverse(l.s)) = r.v WHERE reverse(l.s) != '')
    SETTINGS query_plan_merge_expressions = 0;

SELECT '13. without a join: the same function over an alias of the same column is computed once';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a FROM (SELECT toString(k) AS a, k AS y FROM l) AS t WHERE toString(t.y) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT a FROM (SELECT toString(k) AS a, k AS y FROM l) AS t WHERE toString(t.y) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT a FROM (SELECT toString(k) AS a, k AS y FROM l) AS t WHERE toString(t.y) != '3')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION plus(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a FROM (SELECT plus(k, 1) AS a, k AS y FROM l) AS t WHERE plus(t.y, 1) != 3);

SELECT '14. not reused: a column in WHERE guards the expression';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.ok AND toUInt64(lg.s) > 0 ORDER BY lg.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.ok AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.ok AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;

SELECT '15. not reused: single-step PREWHERE guards the expression';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.s != 'abc' AND toUInt64(lg.s) > 0 ORDER BY lg.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    allow_reorder_prewhere_conditions = 0, enable_multiple_prewhere_read_steps = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.s != 'abc' AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    allow_reorder_prewhere_conditions = 0, enable_multiple_prewhere_read_steps = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.s != 'abc' AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    allow_reorder_prewhere_conditions = 0, enable_multiple_prewhere_read_steps = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;

SELECT '16. reused: guard-free AND chain';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.id > 1 AND toUInt64(lg.s) > 0 ORDER BY lg.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.id > 1 AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;
SELECT count(), sum(cityHash64(*)) FROM (SELECT lg.id, rg.id FROM lg JOIN rg ON toUInt64(lg.s) = rg.id WHERE lg.id > 1 AND toUInt64(lg.s) > 0 ORDER BY lg.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, query_plan_merge_filters = 1,
    query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0;

SELECT '17. null-safe key over a Nullable expression';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ns, rn.id FROM (SELECT id, n, toString(n) AS ns FROM ln) AS a JOIN rn ON concat(toString(a.n), '_') <=> rn.vn);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ns, rn.id FROM (SELECT id, n, toString(n) AS ns FROM ln) AS a JOIN rn ON concat(toString(a.n), '_') <=> rn.vn);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ns, rn.id FROM (SELECT id, n, toString(n) AS ns FROM ln) AS a JOIN rn ON concat(toString(a.n), '_') <=> rn.vn)
    SETTINGS query_plan_merge_expressions = 0;

SELECT '18. not reused: an existing PREWHERE guards the expression in a one-step PREWHERE';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, rg.id FROM (SELECT id, s FROM lg PREWHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id WHERE toUInt64(a.s) > 0 ORDER BY a.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 0, optimize_prewhere_after_pushdown = 1);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg PREWHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 0, optimize_prewhere_after_pushdown = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg PREWHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 0, optimize_prewhere_after_pushdown = 1;

SELECT '19. with join runtime filters';
SELECT countIf(explain ILIKE '%__applyFilter%') > 0 FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != ''
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != ''
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != '')
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r ON JSONExtractString(toString(l.j), 'b') = r.v WHERE JSONExtractString(toString(l.j), 'a') != '')
    SETTINGS enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_merge_expressions = 0;

SELECT '20. not reused: a subquery turns multi-step PREWHERE off for its own read';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, rg.id FROM (SELECT id, s FROM lg SETTINGS enable_multiple_prewhere_read_steps = 0) AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE a.s != 'abc' AND toUInt64(a.s) > 0 ORDER BY a.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg SETTINGS enable_multiple_prewhere_read_steps = 0) AS a
    JOIN rg ON toUInt64(a.s) = rg.id WHERE a.s != 'abc' AND toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0;
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg SETTINGS enable_multiple_prewhere_read_steps = 0) AS a
    JOIN rg ON toUInt64(a.s) = rg.id WHERE a.s != 'abc' AND toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0;

SELECT '21. not reused: with query_plan_merge_filters = 0 a later merge can put another filter in front';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, rg.id FROM (SELECT id, s FROM lg WHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id WHERE toUInt64(a.s) > 0 ORDER BY a.id
    SETTINGS enable_analyzer = 1, query_plan_merge_filters = 0, query_plan_merge_expressions = 1, optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0, join_algorithm = 'hash',
    query_plan_join_swap_table = 'false');
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg WHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, query_plan_merge_filters = 0, query_plan_merge_expressions = 1, optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0, join_algorithm = 'hash',
    query_plan_join_swap_table = 'false';
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, s FROM lg WHERE s != 'abc') AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE toUInt64(a.s) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, query_plan_merge_filters = 0, query_plan_merge_expressions = 0, optimize_move_to_prewhere = 0,
    query_plan_optimize_prewhere = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0, join_algorithm = 'hash',
    query_plan_join_swap_table = 'false';

SELECT '22. not reused: a filter over a read that is not MergeTree';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT lm.id, rg.id FROM lm JOIN rg ON toUInt64(lm.s) = rg.id WHERE lm.id > 1 AND toUInt64(lm.s) > 0 ORDER BY lm.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1);
SELECT count(), sum(cityHash64(*)) FROM (SELECT lm.id, rg.id FROM lm JOIN rg ON toUInt64(lm.s) = rg.id WHERE lm.id > 1 AND toUInt64(lm.s) > 0 ORDER BY lm.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT lm.id, rg.id FROM lm JOIN rg ON toUInt64(lm.s) = rg.id WHERE lm.id > 1 AND toUInt64(lm.s) > 0 ORDER BY lm.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 0, short_circuit_function_evaluation = 'enable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1;

SELECT '23. not reused: a subquery enables short-circuit evaluation for its own read';
SELECT countIf(explain ILIKE '%FUNCTION toUInt64(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, rg.id FROM (SELECT id, ok, s FROM lg SETTINGS short_circuit_function_evaluation = 'enable') AS a JOIN rg ON toUInt64(a.s) = rg.id
    WHERE if(a.ok, toUInt64(a.s), 0) > 0 ORDER BY a.id
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'disable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, rg.id FROM (SELECT id, ok, s FROM lg SETTINGS short_circuit_function_evaluation = 'enable') AS a
    JOIN rg ON toUInt64(a.s) = rg.id WHERE if(a.ok, toUInt64(a.s), 0) > 0 ORDER BY a.id)
    SETTINGS enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1,
    query_plan_merge_filters = 1, query_plan_merge_expressions = 1, short_circuit_function_evaluation = 'disable', enable_join_runtime_filters = 0,
    query_plan_join_swap_table = 'false', enable_multiple_prewhere_read_steps = 1;

SELECT '24. the key is computed after a filter pushed below the join later';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) = rg2.id WHERE a.v != 'abc' ORDER BY a.id
    SETTINGS query_plan_split_filter = 0);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) = rg2.id WHERE a.v != 'abc')
    SETTINGS query_plan_split_filter = 0, query_plan_merge_expressions = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) = rg2.id WHERE a.v != 'abc')
    SETTINGS query_plan_split_filter = 0, query_plan_merge_expressions = 0;

SELECT '25. the same with a stateful function in the key';
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) + rowNumberInAllBlocks() = rg2.id WHERE a.v = '5' ORDER BY a.id
    SETTINGS max_threads = 1);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) + rowNumberInAllBlocks() = rg2.id WHERE a.v = '5')
    SETTINGS max_threads = 1, query_plan_merge_expressions = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ids, rg2.id FROM (SELECT id, any(s) AS v, toString(id) AS ids FROM lg GROUP BY id) AS a
    JOIN rg2 ON toUInt64(concat(toString(a.id), a.v)) + rowNumberInAllBlocks() = rg2.id WHERE a.v = '5')
    SETTINGS max_threads = 1, query_plan_merge_expressions = 0;

SELECT '26. a key with arrayJoin over the reused expression';
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a
    JOIN r ON concat(arrayJoin([toString(a.k)]), toString(a.k)) = r.v)
    SETTINGS query_plan_merge_expressions = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a
    JOIN r ON concat(arrayJoin([toString(a.k)]), toString(a.k)) = r.v)
    SETTINGS query_plan_merge_expressions = 0;

SELECT '27. a key used by two conditions is computed once';
SELECT countIf(explain ILIKE '%FUNCTION rand(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON toString(rand(toString(l.k)) % 1) = r.v AND toString(rand(toString(l.k)) % 1) = toString(r.id)
    WHERE toString(l.k) != '3');
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r
    ON toString(rand(toString(l.k)) % 1) = r.v AND toString(rand(toString(l.k)) % 1) = toString(r.id) WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 1;
SELECT count(), sum(cityHash64(*)) FROM (SELECT l.id, r.id FROM l JOIN r
    ON toString(rand(toString(l.k)) % 1) = r.v AND toString(rand(toString(l.k)) % 1) = toString(r.id) WHERE toString(l.k) != '3')
    SETTINGS query_plan_merge_expressions = 0;
SELECT countIf(explain ILIKE '%FUNCTION concat(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v AND concat(toString(l.k), '_') = toString(r.id)
    WHERE toString(l.k) != '3');
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v AND concat(toString(l.k), '_') = toString(r.id)
    WHERE toString(l.k) != '3');
SELECT countIf(explain ILIKE '%FUNCTION toString(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT l.id, r.id FROM l JOIN r ON concat(toString(l.k), '_') = r.v AND concat(toString(l.k), '_') = toString(r.id)
    WHERE toString(l.k) != '3' SETTINGS query_plan_merge_expressions = 0);

SELECT '28. not reused: the subquery computes arithmetic with different settings';
SELECT countIf(explain ILIKE '%FUNCTION multiply(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, r.id FROM (SELECT id, k, toDecimal32(k, 0) * 10 AS y FROM l SETTINGS decimal_check_overflow = 0) AS a
    JOIN r ON toString(toDecimal32(a.k, 0) * 10) = r.v);
SELECT countIf(explain ILIKE '%FUNCTION sqr(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, r.id FROM (SELECT id, k, sqr(toDecimal32(k, 0)) AS y FROM l SETTINGS decimal_check_overflow = 0) AS a
    JOIN r ON toString(sqr(toDecimal32(a.k, 0))) = r.v);
SELECT countIf(explain ILIKE '%FUNCTION plus(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.t, rdt.t FROM (SELECT id, dd, dd + toTime('23:59:59') AS t FROM ldt SETTINGS date_time_overflow_behavior = 'saturate') AS a
    JOIN rdt ON a.dd + toTime('23:59:59') = rdt.t) SETTINGS date_time_overflow_behavior = 'ignore';
SELECT a.id, a.t = rdt.t FROM (SELECT id, dd, dd + toTime('23:59:59') AS t FROM ldt SETTINGS date_time_overflow_behavior = 'saturate') AS a
    JOIN rdt ON a.dd + toTime('23:59:59') = rdt.t ORDER BY a.id SETTINGS date_time_overflow_behavior = 'ignore';
SELECT a.id, a.t = rdt.t FROM (SELECT id, dd, dd + toTime('23:59:59') AS t FROM ldt SETTINGS date_time_overflow_behavior = 'saturate') AS a
    JOIN rdt ON a.dd + toTime('23:59:59') = rdt.t ORDER BY a.id SETTINGS date_time_overflow_behavior = 'ignore', query_plan_merge_expressions = 0;

SELECT '29. not reused: a function that must run once is shared with an ASOF inequality';
SELECT countIf(explain ILIKE '%FUNCTION rand(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a
    ASOF JOIN r ON toString(rand(toString(a.k)) % 1) = r.v AND a.id + rand(toString(a.k)) % 1 >= r.id);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a
    ASOF JOIN r ON toString(rand(toString(a.k)) % 1) = r.v AND a.id + rand(toString(a.k)) % 1 >= r.id);
SELECT count(), sum(cityHash64(*)) FROM (SELECT a.id, a.ks, r.id FROM (SELECT id, k, toString(k) AS ks FROM l) AS a
    ASOF JOIN r ON toString(rand(toString(a.k)) % 1) = r.v AND a.id + rand(toString(a.k)) % 1 >= r.id)
    SETTINGS query_plan_merge_expressions = 0;

SELECT '30. not reused: a constant that compares equal but is a different value';
SELECT countIf(explain ILIKE '%FUNCTION plus(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, rz.v FROM (SELECT id, x, x + -0. AS y FROM lz) AS a JOIN rz ON toString(a.x + 0.) = rz.v);
SELECT countIf(explain ILIKE '%FUNCTION plus(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, rz.v FROM (SELECT id, x, x + 0. AS y FROM lz) AS a JOIN rz ON toString(a.x + 0.) = rz.v);
SELECT a.id, toString(a.y), rz.v FROM (SELECT id, x, x + -0. AS y FROM lz) AS a JOIN rz ON toString(a.x + 0.) = rz.v
    ORDER BY a.id;
SELECT a.id, toString(a.y), rz.v FROM (SELECT id, x, x + -0. AS y FROM lz) AS a JOIN rz ON toString(a.x + 0.) = rz.v
    ORDER BY a.id SETTINGS query_plan_merge_expressions = 0;

SELECT '31. not reused: a function that does not hash a setting it captured, computed with a different setting';
SELECT countIf(explain ILIKE '%FUNCTION IPv4StringToNum(%') FROM (EXPLAIN actions = 1, compact = 0
    SELECT a.id, a.y, r.v FROM (SELECT 1 AS id, IPv4StringToNum(materialize('bad')) AS y SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 1) AS a
    JOIN (SELECT '1_0' AS v) AS r ON concat(toString(a.id), '_', toString(IPv4StringToNum(materialize('bad')))) = r.v);
SELECT a.id, a.y, r.v FROM (SELECT 1 AS id, IPv4StringToNum(materialize('bad')) AS y SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 1) AS a
    JOIN (SELECT '1_0' AS v) AS r ON concat(toString(a.id), '_', toString(IPv4StringToNum(materialize('bad')))) = r.v; -- { serverError CANNOT_PARSE_IPV4 }
SELECT a.id, a.y, r.v FROM (SELECT 1 AS id, IPv4StringToNum(materialize('bad')) AS y SETTINGS cast_ipv4_ipv6_default_on_conversion_error = 1) AS a
    JOIN (SELECT '1_0' AS v) AS r ON concat(toString(a.id), '_', toString(IPv4StringToNum(materialize('bad')))) = r.v
    SETTINGS query_plan_merge_expressions = 0; -- { serverError CANNOT_PARSE_IPV4 }

DROP TABLE l;
DROP TABLE r;
DROP TABLE lg;
DROP TABLE rg;
DROP TABLE rg2;
DROP TABLE ln;
DROP TABLE rn;
DROP TABLE lm;
DROP TABLE ldt;
DROP TABLE rdt;
DROP TABLE lz;
DROP TABLE rz;
