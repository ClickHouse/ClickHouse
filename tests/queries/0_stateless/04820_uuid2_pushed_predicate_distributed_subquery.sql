-- Tags: shard, no-parallel-replicas
-- no-parallel-replicas: the test pins the exact plan shape of `ReadFromRemote` (like 04278).

-- When a `view(...)` table function wraps a `remote(...)` subquery, the outer `WHERE` predicate is
-- pushed into the remote query by `tryBuildAdditionalFilterAST` in `ReadFromRemote.cpp`, which
-- rebuilds constant DAG nodes as `_CAST(<literal>, '<type>')`. A `UUID2` constant shares the `Field`
-- representation with `UUID` (with the two 64-bit halves in the opposite order), and a raw literal is
-- always formatted with `UUID` semantics, so the remote shard used to reparse a different `UUID2`
-- value and the pushed predicate silently matched nothing.

SET enable_analyzer = 1;
SET allow_push_predicate_ast_for_distributed_subqueries = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS data_uuid2_04820;

CREATE TABLE data_uuid2_04820 (id UUID2, id1 UUID) ENGINE = MergeTree ORDER BY id;

-- The value must not be symmetric under a half swap, or the buggy layout round trip is invisible.
INSERT INTO data_uuid2_04820 VALUES ('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29', '4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), ('df195aeb-b02c-43ef-a626-91144d58eee5', 'df195aeb-b02c-43ef-a626-91144d58eee5');

-- Both "shards" of `remote('127.0.0.{1,2}', ...)` are the same local table and
-- `prefer_localhost_replica = 0` sends the query to both over the wire, so the matching row must
-- come back from each of them: `2` proves the pushed predicate matched on the remote side, while the
-- broken literal round trip returned `0`.

SELECT 'UUID2 column, UUID2 constant';
SELECT count()
FROM view(SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), data_uuid2_04820))
WHERE id = toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29');

SELECT 'UUID column, UUID2 constant';
SELECT count()
FROM view(SELECT id1 FROM remote('127.0.0.{1,2}', currentDatabase(), data_uuid2_04820))
WHERE id1 = toUUID2('df195aeb-b02c-43ef-a626-91144d58eee5');

SELECT 'UUID2 column, UUID constant';
SELECT count()
FROM view(SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), data_uuid2_04820))
WHERE id = toUUID('df195aeb-b02c-43ef-a626-91144d58eee5');

SELECT 'UUID2 column, IN with UUID2 constants';
SELECT count()
FROM view(SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), data_uuid2_04820))
WHERE id IN (toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29'), toUUID2('df195aeb-b02c-43ef-a626-91144d58eee5'));

-- The pushed-down filter must actually appear inside the remote-side plan (the line above
-- `ReadFromRemote` plus the one inside its remote plan), otherwise the counts above would pass
-- trivially with pushdown silently dropped.
SELECT 'pushed filter is in the remote plan';
SELECT countIf(explain ILIKE '%Filter column: equals(%') >= 2
FROM
(
    EXPLAIN actions = 1, distributed = 1
    SELECT id FROM view(SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), data_uuid2_04820))
    WHERE id = toUUID2('4556d7a4-d7ef-4b2a-9cb2-8759a9ea5e29')
);

DROP TABLE data_uuid2_04820;
