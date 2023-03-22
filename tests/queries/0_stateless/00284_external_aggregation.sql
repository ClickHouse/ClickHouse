-- Tags: long

SET max_bytes_before_external_group_by = 100000000;
SET max_memory_usage = 410000000;
SET group_by_two_level_threshold = 100000;
SET group_by_two_level_threshold_bytes = 50000000;

SELECT sum(k), sum(c) FROM (SELECT number AS k, count() AS c FROM (SELECT * FROM system.numbers LIMIT 10000000) GROUP BY k);
SELECT sum(k), sum(c), max(u) FROM (SELECT number AS k, count() AS c, uniqArray(range(number % 16)) AS u FROM (SELECT * FROM system.numbers LIMIT 1000000) GROUP BY k);

SET group_by_two_level_threshold = 100000;
SET max_bytes_before_external_group_by = '1Mi';

-- method: key_string & key_string_two_level
CREATE TABLE t_00284_str(s String) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_00284_str SELECT toString(number) FROM numbers_mt(1e6);
INSERT INTO t_00284_str SELECT toString(number) FROM numbers_mt(1e6);
SELECT s, count() FROM t_00284_str GROUP BY s ORDER BY s LIMIT 10 OFFSET 42;

-- method: low_cardinality_key_string & low_cardinality_key_string_two_level
CREATE TABLE t_00284_lc_str(s LowCardinality(String)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t_00284_lc_str SELECT toString(number) FROM numbers_mt(1e6);
INSERT INTO t_00284_lc_str SELECT toString(number) FROM numbers_mt(1e6);
SELECT s, count() FROM t_00284_lc_str GROUP BY s ORDER BY s LIMIT 10 OFFSET 42;
