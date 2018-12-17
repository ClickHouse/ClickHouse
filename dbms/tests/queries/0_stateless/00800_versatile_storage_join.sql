DROP TABLE IF EXISTS test.join_any_inner;
DROP TABLE IF EXISTS test.join_any_left;
DROP TABLE IF EXISTS test.join_any_left_null;
DROP TABLE IF EXISTS test.join_all_inner;
DROP TABLE IF EXISTS test.join_all_left;
DROP TABLE IF EXISTS test.join_string_key;

CREATE TABLE test.join_any_inner (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, INNER, k);
CREATE TABLE test.join_any_left (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE test.join_all_inner (s String, x Array(UInt8), k UInt64) ENGINE = Join(ALL, INNER, k);
CREATE TABLE test.join_all_left (s String, x Array(UInt8), k UInt64) ENGINE = Join(ALL, LEFT, k);

USE test;

INSERT INTO test.join_any_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO test.join_any_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO test.join_all_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO test.join_all_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);

-- read from StorageJoin

SELECT '--------read--------';
SELECT * from test.join_any_inner;
SELECT * from test.join_any_left;
SELECT * from test.join_all_inner;
SELECT * from test.join_all_left;

-- create StorageJoin tables with customized settings

CREATE TABLE test.join_any_left_null (s String, k UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO test.join_any_left_null VALUES ('abc', 1), ('def', 2);

-- joinGet
SELECT '--------joinGet--------';
SELECT joinGet('join_any_left', 's', number) FROM numbers(3);
SELECT '';
SELECT joinGet('join_any_left_null', 's', number) FROM numbers(3);
SELECT '';

CREATE TABLE test.join_string_key (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, s);
INSERT INTO test.join_string_key VALUES ('abc', [0], 1), ('def', [1, 2], 2);
SELECT joinGet('join_string_key', 'x', 'abc'), joinGet('join_string_key', 'k', 'abc');

USE default;

DROP TABLE test.join_any_inner;
DROP TABLE test.join_any_left;
DROP TABLE test.join_any_left_null;
DROP TABLE test.join_all_inner;
DROP TABLE test.join_all_left;
DROP TABLE test.join_string_key;
