DROP TABLE IF EXISTS test_joinGet;
DROP TABLE IF EXISTS test_join_joinGet;

CREATE TABLE test_joinGet(id Int32, user_id Nullable(Int32)) Engine = Memory();
CREATE TABLE test_join_joinGet(user_id Int32, name String) Engine = Join(ANY, LEFT, user_id);

INSERT INTO test_join_joinGet VALUES (2, 'a'), (6, 'b'), (10, 'c');

SELECT 2 id, toNullable(toInt32(2)) user_id WHERE joinGet(test_join_joinGet, 'name', user_id) != '';

DROP TABLE test_joinGet;
DROP TABLE test_join_joinGet;
