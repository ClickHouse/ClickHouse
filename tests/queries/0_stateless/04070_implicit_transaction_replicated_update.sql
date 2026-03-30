-- Tags: no-fasttest

DROP TABLE IF EXISTS test.t_04070_itr SYNC;

CREATE TABLE test.t_04070_itr (id UInt64, val String)
ENGINE = ReplicatedMergeTree('/clickhouse/test/t_04070_itr_98371/r1', 'r1')
ORDER BY id;

INSERT INTO test.t_04070_itr VALUES (1, 'a'), (2, 'b'), (3, 'c');

ALTER TABLE test.t_04070_itr UPDATE val = 'x' WHERE id = 1 SETTINGS mutations_sync=2, implicit_transaction=1, throw_on_unsupported_query_inside_transaction=0; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE test.t_04070_itr DELETE WHERE id = 3 SETTINGS mutations_sync=2, implicit_transaction=1, throw_on_unsupported_query_inside_transaction=0; -- { serverError NOT_IMPLEMENTED }

SELECT id, val FROM test.t_04070_itr ORDER BY id;

ALTER TABLE test.t_04070_itr UPDATE val = 'x' WHERE id = 1 SETTINGS mutations_sync=2;
SELECT id, val FROM test.t_04070_itr ORDER BY id;

DROP TABLE IF EXISTS test.t_04070_itr SYNC;
