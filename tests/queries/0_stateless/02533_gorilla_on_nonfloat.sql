DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, val Decimal(15,5) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (id UInt64, val FixedString(2) CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (id UInt64, val UInt64 CODEC (Gorilla)) ENGINE = MergeTree() ORDER BY id; -- { serverError BAD_ARGUMENTS }
