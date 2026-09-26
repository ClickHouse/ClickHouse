-- Regression test: FunctionToSubcolumnsPass must not rewrite m['key'] -> m.key_<key>
-- in post-aggregation clauses (HAVING, ORDER BY, SELECT, LIMIT BY) when the full map
-- m is also read (e.g. GROUP BY m), because the resulting subcolumn would not be in
-- GROUP BY and would cause an exception.
--
-- The optimization must still apply in WHERE/PREWHERE (pre-aggregation).
-- Every query is run with optimize_functions_to_subcolumns = 1 and = 0;
-- results must match.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_map_having;

CREATE TABLE t_map_having (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1;

INSERT INTO t_map_having SELECT number, map('a', number, 'b', number * 10, 'c', number * 100) FROM numbers(10);
OPTIMIZE TABLE t_map_having FINAL;

-- HAVING without WHERE: must not rewrite (no filter-only optimization triggered)
SELECT 'HAVING only';
SELECT m FROM t_map_having GROUP BY m HAVING m['a'] > 5 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having GROUP BY m HAVING m['a'] > 5 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + HAVING same key: must rewrite in WHERE but NOT in HAVING
SELECT 'WHERE + HAVING same key';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + HAVING different key
SELECT 'WHERE + HAVING different key';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING m['b'] < 90 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING m['b'] < 90 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + ORDER BY same key (post-aggregation)
SELECT 'WHERE + ORDER BY same key';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + ORDER BY different key
SELECT 'WHERE + ORDER BY different key';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['b']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['b']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + SELECT m['key'] (post-aggregation)
SELECT 'WHERE + SELECT same key';
SELECT m['a'] FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m['a'] FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + SELECT m['different key']
SELECT 'WHERE + SELECT different key';
SELECT m['b'] FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m['b'] FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + LIMIT BY same key
SELECT 'WHERE + LIMIT BY same key';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a'] LIMIT 1 BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a'] LIMIT 1 BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- PREWHERE + HAVING same key
SELECT 'PREWHERE + HAVING same key';
SELECT m FROM t_map_having PREWHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having PREWHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + HAVING length(m) — other transformers on the same column must not be
-- rewritten in post-aggregation clauses either
SELECT 'WHERE + HAVING length(m)';
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING length(m) > 2 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_having WHERE m['a'] > 5 GROUP BY m HAVING length(m) > 2 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE + SELECT mapKeys(m) — mapKeys transformer must not fire in SELECT
SELECT 'WHERE + SELECT mapKeys';
SELECT mapKeys(m) FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT mapKeys(m) FROM t_map_having WHERE m['a'] > 5 GROUP BY m ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- Memory engine: confirms bug is in the analyzer, not serialization
SELECT 'Memory engine WHERE + HAVING';
DROP TABLE IF EXISTS t_map_mem;
CREATE TABLE t_map_mem (id UInt64, m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO t_map_mem SELECT number, map('a', number, 'b', number * 10, 'c', number * 100) FROM numbers(10);

SELECT m FROM t_map_mem WHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m FROM t_map_mem WHERE m['a'] > 5 GROUP BY m HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_map_mem;

-- Negative: GROUP BY subcolumn (should still work — all uses are transformable)
SELECT 'Negative: GROUP BY subcolumn';
SELECT m['a'], count() FROM t_map_having WHERE m['a'] > 5 GROUP BY m['a'] HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;
SELECT m['a'], count() FROM t_map_having WHERE m['a'] > 5 GROUP BY m['a'] HAVING m['a'] < 9 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 0;

-- WHERE optimization must still work (regression guard)
SELECT 'WHERE optimization still works';
SELECT m['a'] FROM t_map_having WHERE m['a'] > 5 ORDER BY m['a']
    SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_map_having;
