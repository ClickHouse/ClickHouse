CREATE TABLE test_max_size_drop
Engine = MergeTree()
ORDER BY number
AS SELECT number
FROM numbers(1000)
;

DROP TABLE test_max_size_drop SETTINGS max_table_size_to_drop = 1; -- { serverError 359 }
DROP TABLE test_max_size_drop;

CREATE TABLE test_max_size_drop
Engine = MergeTree()
ORDER BY number
AS SELECT number
FROM numbers(1000)
;

ALTER TABLE test_max_size_drop DROP PARTITION tuple() SETTINGS max_partition_size_to_drop = 1; -- { serverError 359 }
ALTER TABLE test_max_size_drop DROP PARTITION tuple();
DROP TABLE test_max_size_drop;

CREATE TABLE test_max_size_drop
Engine = MergeTree()
ORDER BY number
AS SELECT number
FROM numbers(1000)
;

ALTER TABLE test_max_size_drop DROP PART 'all_1_1_0' SETTINGS max_partition_size_to_drop = 1; -- { serverError 359 }
ALTER TABLE test_max_size_drop DROP PART 'all_1_1_0';
DROP TABLE test_max_size_drop;
