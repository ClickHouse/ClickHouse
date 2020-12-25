DROP TABLE IF EXISTS test;
CREATE TABLE test (a Float32, b int) Engine = MergeTree() ORDER BY tuple() PARTITION BY a; -- { serverError 36 }
CREATE TABLE test (a Float32, b int, c String, d Float64) Engine = MergeTree() ORDER BY tuple() PARTITION BY (b, c, d); -- { serverError 36 }
