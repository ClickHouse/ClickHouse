DROP TABLE IF EXISTS join_test;

CREATE TABLE join_test (id UInt16, num UInt16) engine = Join(ANY, LEFT, id) settings join_any_take_last_row = 1;

INSERT INTO join_test (id, num) SELECT number, number FROM system.numbers LIMIT 1000;

SELECT joinGet('join_test', 'num', 500);

-- joinGet('join_test', 'num', 500) will be 500 and it is fine
-- replace all the values

INSERT INTO join_test (id, num) SELECT number, number * 2 FROM system.numbers LIMIT 1000;

SELECT joinGet ('join_test', 'num', 500);

-- joinGet('join_test', 'num', 500) will be 1000 and it is fine

TRUNCATE TABLE join_test;

INSERT INTO join_test (id, num) SELECT number, number FROM system.numbers LIMIT 1000;

INSERT INTO join_test (id, num) SELECT number, number * 2 FROM system.numbers LIMIT 1000;

SELECT joinGet('join_test', 'num', 500);

-- joinGet('join_test', 'num', 500) will be 1000 and it is not fine
DROP TABLE join_test;
