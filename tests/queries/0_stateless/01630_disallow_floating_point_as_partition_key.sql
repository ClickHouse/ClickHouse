DROP TABLE IF EXISTS test;
CREATE TABLE test (a Float32, b int) Engine = MergeTree() ORDER BY tuple() PARTITION BY a; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (a Float32, b int) Engine = MergeTree() ORDER BY tuple() PARTITION BY a settings allow_floating_point_partition_key=true;
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Float32, b int, c String, d Float64) Engine = MergeTree() ORDER BY tuple() PARTITION BY (b, c, d) settings allow_floating_point_partition_key=false; -- { serverError BAD_ARGUMENTS }
CREATE TABLE test (a Float32, b int, c String, d Float64) Engine = MergeTree() ORDER BY tuple() PARTITION BY (b, c, d) settings allow_floating_point_partition_key=true;
DROP TABLE IF EXISTS test;
