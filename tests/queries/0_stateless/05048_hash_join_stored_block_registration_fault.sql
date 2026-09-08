-- Tags: no-fasttest, no-parallel, no-parallel-replicas
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once, so a concurrent copy of this test
-- consumes the trigger and the first insert below then succeeds.
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS t_join_registration;
-- `join_any_take_last_row = 0` keeps the first row of a key, so re-inserting a key is a no-op.
CREATE TABLE t_join_registration (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_any_take_last_row = 0;

SYSTEM ENABLE FAILPOINT stored_columns_index_throw_on_add;
INSERT INTO t_join_registration VALUES (1, 1); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT stored_columns_index_throw_on_add;

-- The table survives the failed insert, so every statement below reads state it left behind.
INSERT INTO t_join_registration VALUES (2, 2);
-- Key 2 again: nothing is inserted, so the block is discarded after it was already registered.
INSERT INTO t_join_registration VALUES (2, 3);
SELECT k, v FROM t_join_registration ORDER BY k;

-- The right-side row count must not have been advanced by the failed insert either.
SELECT extract(explain, 'Right: rows ([0-9]+)') AS right_rows
FROM (EXPLAIN ANALYZE SELECT count() FROM (SELECT number AS k FROM numbers(3)) AS l
          ANY LEFT JOIN t_join_registration AS r ON l.k = r.k)
WHERE explain LIKE '%Right: rows%';

DROP TABLE t_join_registration;
