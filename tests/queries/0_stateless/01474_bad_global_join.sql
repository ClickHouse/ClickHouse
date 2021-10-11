-- Tags: global

DROP TABLE IF EXISTS local_table;
DROP TABLE IF EXISTS dist_table;

CREATE TABLE local_table (id UInt64, val String) ENGINE = Memory;

INSERT INTO local_table SELECT number AS id, toString(number) AS val FROM numbers(100);

CREATE TABLE dist_table AS local_table
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), local_table);

SELECT uniq(d.val) FROM dist_table AS d GLOBAL LEFT JOIN numbers(100) AS t USING id; -- { serverError 284 }
SELECT uniq(d.val) FROM dist_table AS d GLOBAL LEFT JOIN local_table AS t USING id;

DROP TABLE local_table;
DROP TABLE dist_table;
