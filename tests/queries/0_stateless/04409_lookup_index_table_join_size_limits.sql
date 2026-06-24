-- A `table_join` LOOKUP INDEX reads and caches the whole right side once, and later cache hits reuse
-- it without re-checking; it therefore cannot honor the per-query right-side join size limits that the
-- regular join build enforces. When `max_rows_in_join` / `max_bytes_in_join` are active, the lookup
-- fast path must fall back to the regular join so those limits are preserved.

SET enable_analyzer = 1;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_parallel_replicas = 1;
SET allow_experimental_lookup_index = 1;
SET join_algorithm = 'direct,hash';

DROP TABLE IF EXISTS t_lookup_limit_dim SYNC;
DROP TABLE IF EXISTS t_lookup_limit_fact SYNC;

CREATE TABLE t_lookup_limit_dim
(
    id UInt64,
    val String,
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE t_lookup_limit_fact
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_lookup_limit_dim VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_lookup_limit_fact VALUES (1, 'x'), (2, 'y'), (3, 'z');

-- Without size limits, the lookup fast path is used.
SELECT 'no limit: using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM t_lookup_limit_fact AS f
    INNER ALL JOIN t_lookup_limit_dim AS d USING (id)
);

-- With max_rows_in_join active, the plan must fall back (no DirectKeyValueJoin) so the limit applies.
SELECT 'max_rows_in_join: not using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM t_lookup_limit_fact AS f
    INNER ALL JOIN t_lookup_limit_dim AS d USING (id)
    SETTINGS max_rows_in_join = 2
);

-- The regular join now enforces the limit instead of silently serving from the lookup cache.
SELECT 'max_rows_in_join enforced';
SELECT f.id, d.val
FROM t_lookup_limit_fact AS f
INNER ALL JOIN t_lookup_limit_dim AS d USING (id)
SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- max_bytes_in_join likewise forces the fallback.
SELECT 'max_bytes_in_join: not using DirectKeyValueJoin';
SELECT countIf(explain LIKE '%Algorithm: DirectKeyValueJoin%')
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT f.id, d.val
    FROM t_lookup_limit_fact AS f
    INNER ALL JOIN t_lookup_limit_dim AS d USING (id)
    SETTINGS max_bytes_in_join = 1
);

DROP TABLE t_lookup_limit_fact SYNC;
DROP TABLE t_lookup_limit_dim SYNC;
