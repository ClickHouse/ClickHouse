SET enable_analyzer = 1;
SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_parallel_replicas = 1;
-- `limits.yaml` in the standard stateless config sets `max_rows_in_join` / `max_bytes_in_join` to a high
-- ceiling; reset them so the `table_join` lookup fast path (declined on any non-zero limit) is exercised.
SET max_rows_in_join = 0, max_bytes_in_join = 0;
SET allow_experimental_lookup_index = 1;

DROP TABLE IF EXISTS t_lookup_take_last_probe SYNC;
DROP TABLE IF EXISTS t_lookup_take_last_dim SYNC;
DROP TABLE IF EXISTS t_lookup_take_last_dim_noidx SYNC;

CREATE TABLE t_lookup_take_last_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lookup_take_last_probe VALUES (1);

CREATE TABLE t_lookup_take_last_dim
(
    id UInt64,
    val String,
    LOOKUP INDEX idx_join (id) TYPE table_join
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO t_lookup_take_last_dim VALUES (1, 'first'), (1, 'last');

CREATE TABLE t_lookup_take_last_dim_noidx
(
    id UInt64,
    val String
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO t_lookup_take_last_dim_noidx VALUES (1, 'first'), (1, 'last');

-- A `table_join` lookup index serves an `ANY` join via a key-value direct join that always returns
-- the first stored right row for a key. `join_any_take_last_row = 1` instead asks an `ANY` join to
-- keep the last duplicate-key right row, which only the regular `HashJoin` honors. The lookup fast
-- path must be declined for `ANY` joins under that setting, otherwise enabling the index would
-- silently change query results.

-- Without the setting, the `ANY` join uses the lookup index (a `DirectKeyValueJoin`).
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim USING (id)
      SETTINGS join_any_take_last_row = 0)
WHERE explain LIKE '%DirectKeyValueJoin%';

-- With the setting, the lookup is declined and the regular join path is used (no `DirectKeyValueJoin`).
SELECT count()
FROM (EXPLAIN actions = 1 SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim USING (id)
      SETTINGS join_any_take_last_row = 1)
WHERE explain LIKE '%DirectKeyValueJoin%';

-- Same for `ANY LEFT JOIN`.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT val FROM t_lookup_take_last_probe ANY LEFT JOIN t_lookup_take_last_dim USING (id)
      SETTINGS join_any_take_last_row = 1)
WHERE explain LIKE '%DirectKeyValueJoin%';

-- `ALL` joins are unaffected by `join_any_take_last_row`, so the lookup index is still used.
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT val FROM t_lookup_take_last_probe ALL INNER JOIN t_lookup_take_last_dim USING (id)
      SETTINGS join_any_take_last_row = 1)
WHERE explain LIKE '%DirectKeyValueJoin%';

-- Enabling the lookup index must not change `ANY` join results under `join_any_take_last_row`.
SELECT
    (SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim USING (id) SETTINGS join_any_take_last_row = 1)
  = (SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim_noidx USING (id) SETTINGS join_any_take_last_row = 1);

SELECT
    (SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim USING (id) SETTINGS join_any_take_last_row = 0)
  = (SELECT val FROM t_lookup_take_last_probe ANY INNER JOIN t_lookup_take_last_dim_noidx USING (id) SETTINGS join_any_take_last_row = 0);

DROP TABLE t_lookup_take_last_probe SYNC;
DROP TABLE t_lookup_take_last_dim SYNC;
DROP TABLE t_lookup_take_last_dim_noidx SYNC;
