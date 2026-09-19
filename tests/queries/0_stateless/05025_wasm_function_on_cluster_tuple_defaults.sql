-- Regression test: Wasm function signatures must be validated on the initiator before an
-- `ON CLUSTER` query is sent to workers, like the executable-UDF driver path already does.
-- Tuple-element `DEFAULT` expressions are valid only in column declarations, and older workers
-- would not even parse them.

CREATE FUNCTION wasm_tuple_default_argument_05025
    ON CLUSTER test_shard_localhost
    LANGUAGE WASM
    ARGUMENTS (x Tuple(a UInt8 DEFAULT 1))
    RETURNS UInt8
    FROM 'nonexistent_module_05025'; -- { serverError BAD_ARGUMENTS }

CREATE FUNCTION wasm_tuple_default_return_05025
    ON CLUSTER test_shard_localhost
    LANGUAGE WASM
    ARGUMENTS (x UInt8)
    RETURNS Tuple(b UInt8 DEFAULT 2)
    FROM 'nonexistent_module_05025'; -- { serverError BAD_ARGUMENTS }

-- The single-node worker leg rejects the signature too, so the errors above alone do not prove
-- initiator-side validation. Assert that the queries were never dispatched: no DDL log entry
-- for these function names may exist in the distributed DDL queue.
SELECT count()
FROM system.zookeeper
WHERE path = '/clickhouse/task_queue/ddl'
    AND name LIKE 'query-%'
    AND value LIKE '%wasm_tuple_default_%_05025%';
