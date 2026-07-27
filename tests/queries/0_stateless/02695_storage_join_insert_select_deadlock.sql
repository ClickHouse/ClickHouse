DROP TABLE IF EXISTS test_table_join;

CREATE TABLE test_table_join
(
    id UInt64,
    value String
) ENGINE = Join(Any, Left, id);

INSERT INTO test_table_join VALUES (1, 'q');

-- async_insert=0: these inserts must hold the read lock to trigger DEADLOCK_AVOIDED with themselves.
INSERT INTO test_table_join SELECT * from test_table_join SETTINGS async_insert = 0; -- { serverError DEADLOCK_AVOIDED }

INSERT INTO test_table_join SELECT * FROM (SELECT 1 as id) AS t1 ANY LEFT JOIN test_table_join USING (id) SETTINGS async_insert = 0; -- { serverError DEADLOCK_AVOIDED }
INSERT INTO test_table_join SELECT id, toString(id) FROM (SELECT 1 as id) AS t1 ANY LEFT JOIN (SELECT id FROM test_table_join) AS t2 USING (id) SETTINGS async_insert = 0; -- { serverError DEADLOCK_AVOIDED }

DROP TABLE IF EXISTS test_table_join;
