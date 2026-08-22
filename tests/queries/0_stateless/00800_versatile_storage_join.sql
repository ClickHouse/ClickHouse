
DROP TABLE IF EXISTS join_any_inner;
DROP TABLE IF EXISTS join_any_left;
DROP TABLE IF EXISTS join_any_left_null;
DROP TABLE IF EXISTS join_all_inner;
DROP TABLE IF EXISTS join_all_left;
DROP TABLE IF EXISTS join_string_key;

CREATE TABLE join_any_inner (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, INNER, k);
CREATE TABLE join_any_left (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE join_all_inner (s String, x Array(UInt8), k UInt64) ENGINE = Join(ALL, INNER, k);
CREATE TABLE join_all_left (s String, x Array(UInt8), k UInt64) ENGINE = Join(ALL, LEFT, k);

INSERT INTO join_any_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_any_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_all_inner VALUES ('abc', [0], 1), ('def', [1, 2], 2);
INSERT INTO join_all_left VALUES ('abc', [0], 1), ('def', [1, 2], 2);

-- read from StorageJoin

SELECT '--------read--------';
SELECT * from join_any_inner ORDER BY k;
SELECT * from join_any_left ORDER BY k;
SELECT * from join_all_inner ORDER BY k;
SELECT * from join_all_left ORDER BY k;

-- create StorageJoin tables with customized settings

CREATE TABLE join_any_left_null (s String, k UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_use_nulls = 1;
INSERT INTO join_any_left_null VALUES ('abc', 1), ('def', 2);

-- joinGet
SELECT '--------joinGet--------';
SELECT joinGet('join_any_left', 's', number) FROM numbers(3);
SELECT '';
SELECT joinGet('join_any_left_null', 's', number) FROM numbers(3);
SELECT '';

-- Using identifier as the first argument

SELECT joinGet(join_any_left, 's', number) FROM numbers(3);
SELECT '';
SELECT joinGet(join_any_left_null, 's', number) FROM numbers(3);
SELECT '';

CREATE TABLE join_string_key (s String, x Array(UInt8), k UInt64) ENGINE = Join(ANY, LEFT, s);
INSERT INTO join_string_key VALUES ('abc', [0], 1), ('def', [1, 2], 2);
SELECT joinGet('join_string_key', 'x', 'abc'), joinGet('join_string_key', 'k', 'abc');

USE default;

DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_any_inner;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_any_left;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_any_left_null;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_all_inner;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_all_left;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_string_key;

-- test provided by Alexander Zaitsev
DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE:Identifier}.join_test;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.join_test (a UInt8, b UInt8) Engine = Join(ANY, LEFT, a);

USE {CLICKHOUSE_DATABASE:Identifier};
select joinGet('join_test', 'b', 1);

USE system;
SELECT joinGet({CLICKHOUSE_DATABASE:String} || '.join_test', 'b', 1);

USE default;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.join_test;
