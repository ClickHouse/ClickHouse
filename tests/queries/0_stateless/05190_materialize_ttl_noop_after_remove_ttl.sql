-- Tags: no-replicated-database
-- https://github.com/ClickHouse/ClickHouse/issues/113615
-- A queued MATERIALIZE TTL must not wedge the mutation queue when TTL is removed
-- before the mutation runs (no crash required). Execution degrades to a no-op.

DROP TABLE IF EXISTS t_materialize_ttl_after_remove;
CREATE TABLE t_materialize_ttl_after_remove
(
    id UInt64,
    val UInt64,
    d DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 10 YEAR;

INSERT INTO t_materialize_ttl_after_remove SELECT number, number, now() FROM numbers(10);

-- Queue MATERIALIZE TTL while merges are held, then remove TTL before it runs.
SYSTEM STOP MERGES t_materialize_ttl_after_remove;
ALTER TABLE t_materialize_ttl_after_remove MATERIALIZE TTL SETTINGS mutations_sync = 0;
ALTER TABLE t_materialize_ttl_after_remove REMOVE TTL;
SYSTEM START MERGES t_materialize_ttl_after_remove;

ALTER TABLE t_materialize_ttl_after_remove UPDATE val = val + 1000 WHERE id < 5 SETTINGS mutations_sync = 2;

SELECT countIf(is_done = 0) FROM system.mutations
WHERE database = currentDatabase() AND table = 't_materialize_ttl_after_remove'
  AND latest_fail_reason LIKE '%Cannot MATERIALIZE TTL%';

SELECT sum(val) FROM t_materialize_ttl_after_remove;

-- Submission still refuses MATERIALIZE TTL when the table has no TTL.
ALTER TABLE t_materialize_ttl_after_remove MATERIALIZE TTL; -- { serverError INCORRECT_QUERY }

DROP TABLE t_materialize_ttl_after_remove;
