DROP TABLE IF EXISTS test_joinGet;

CREATE TABLE test_joinGet(user_id Nullable(Int32), name String) Engine = Join(ANY, LEFT, user_id);

INSERT INTO test_joinGet VALUES (2, 'a'), (6, 'b'), (10, 'c'), (null, 'd');

SELECT toNullable(toInt32(2)) user_id WHERE joinGet(test_joinGet, 'name', user_id) != '';

-- If the JOIN keys are Nullable fields, the rows where at least one of the keys has the value NULL are not joined.
SELECT cast(null AS Nullable(Int32)) user_id WHERE joinGet(test_joinGet, 'name', user_id) != '';

DROP TABLE test_joinGet;
