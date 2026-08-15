-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/107619#issuecomment-5230196558.
--
-- `ReadFromRemote` is not clonable. The speculative set build used for primary-key analysis used
-- to consume such a source anyway. When the unavailable shard stopped that build before
-- `Set::finishInsert`, the deferred build had no source left and `FunctionIn` received a not-ready
-- set. A non-clonable source must instead be preserved for the deferred build, whose remote
-- connection error then propagates normally.

DROP TABLE IF EXISTS t_remote_in_04891;

CREATE TABLE t_remote_in_04891 (id UInt64)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES t_remote_in_04891;

SET enable_analyzer = 1;
SET use_index_for_in_with_subqueries = 1;

INSERT INTO t_remote_in_04891 SELECT number FROM numbers(500);
INSERT INTO t_remote_in_04891 SELECT number + 500 FROM numbers(500);
INSERT INTO t_remote_in_04891 SELECT number + 1000 FROM numbers(500);
INSERT INTO t_remote_in_04891 SELECT number + 1500 FROM numbers(500);
INSERT INTO t_remote_in_04891 SELECT number + 2000 FROM numbers(500);

SELECT count()
FROM t_remote_in_04891
WHERE id IN
(
    SELECT id
    FROM clusterAllReplicas('test_unavailable_shard', currentDatabase(), 't_remote_in_04891')
    WHERE (id % 4) = 2
)
SETTINGS max_threads = 1; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DROP TABLE t_remote_in_04891;
