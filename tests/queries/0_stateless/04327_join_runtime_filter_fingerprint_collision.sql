-- A runtime filter's structural fingerprint (the `__applyFilter` const argument NAME,
-- `_runtime_filter_<hash>`, used for EXPLAIN and plan hashing) is intentionally not unique: two
-- structurally identical joins hash to the same fingerprint. Two joins in one FROM do NOT collide --
-- the analyzer gives each occurrence a distinct positional table id (`__table2.key` vs `__table3.key`),
-- hence a distinct fingerprint. But two identical joins in separate scopes (here, scalar subqueries)
-- DO collide: they coexist in one query's `IRuntimeFilterLookup` under the same name. The build/apply
-- rendezvous uses a fresh random key per filter (the const argument VALUE), so a colliding fingerprint
-- must not cross-wire -- applying one subquery's filter to the other would prune the wrong rows. See
-- `joinRuntimeFilter.cpp`.

DROP TABLE IF EXISTS rf_fp_t;
DROP TABLE IF EXISTS rf_fp_a;

CREATE TABLE rf_fp_t (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE rf_fp_a (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rf_fp_t SELECT number, number * 31 + 7 FROM numbers(1000);
-- Two payload rows per key, so the joins are 1-to-many and the aggregates depend on which rows
-- survive and how they pair, not just on a row count.
INSERT INTO rf_fp_a SELECT number, number * 17 + 3 FROM numbers(1000);
INSERT INTO rf_fp_a SELECT number, number * 19 + 5 FROM numbers(1000);

SET enable_analyzer = 1, query_plan_join_swap_table = 'false';
SET explain_query_plan_default = 'legacy';

-- The collision condition: the two identical join shapes really do share a fingerprint name (so when
-- they run together below they land in one registry under that one name).
SELECT 'same fingerprint', (name1 = name2) AND (name1 != '')
FROM
(
    SELECT
        (SELECT extractAll(arrayStringConcat(groupArray(explain), ' '), '_runtime_filter_\\d+')[1]
         FROM (EXPLAIN actions = 1 SELECT count() FROM rf_fp_t JOIN rf_fp_a ON rf_fp_t.k = rf_fp_a.k WHERE rf_fp_a.k < 300 SETTINGS enable_join_runtime_filters = 1)) AS name1,
        (SELECT extractAll(arrayStringConcat(groupArray(explain), ' '), '_runtime_filter_\\d+')[1]
         FROM (EXPLAIN actions = 1 SELECT count() FROM rf_fp_t JOIN rf_fp_a ON rf_fp_t.k = rf_fp_a.k WHERE rf_fp_a.k >= 700 SETTINGS enable_join_runtime_filters = 1)) AS name2
);

-- Both colliding joins run in ONE query, so both filters live in one registry under the same
-- fingerprint name (verified above) but with distinct random rendezvous keys. A cross-wire would apply
-- subquery 1's filter (built from a.k < 300) to subquery 2 (or vice versa) and prune rows that should
-- match. The `count()` and the payload hash (sensitive to every surviving row and pairing) must be
-- identical with runtime filters on (the colliding case) and off.
SELECT 'rf on',
    (SELECT (count(), sum(cityHash64(t.k, t.v, a.w))) FROM rf_fp_t t JOIN rf_fp_a a ON t.k = a.k WHERE a.k < 300),
    (SELECT (count(), sum(cityHash64(t.k, t.v, a.w))) FROM rf_fp_t t JOIN rf_fp_a a ON t.k = a.k WHERE a.k >= 700)
SETTINGS enable_join_runtime_filters = 1;
SELECT 'rf off',
    (SELECT (count(), sum(cityHash64(t.k, t.v, a.w))) FROM rf_fp_t t JOIN rf_fp_a a ON t.k = a.k WHERE a.k < 300),
    (SELECT (count(), sum(cityHash64(t.k, t.v, a.w))) FROM rf_fp_t t JOIN rf_fp_a a ON t.k = a.k WHERE a.k >= 700)
SETTINGS enable_join_runtime_filters = 0;

DROP TABLE rf_fp_t;
DROP TABLE rf_fp_a;
