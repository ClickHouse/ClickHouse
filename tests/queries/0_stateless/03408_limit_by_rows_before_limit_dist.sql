-- Tags: shard

SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS 03408_local;
DROP TABLE IF EXISTS 03408_dist;

CREATE TABLE 03408_local (id Int32, val String) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part=1
AS
SELECT number % 10, leftPad(toString(number), 2, '0') FROM numbers(50);

CREATE TABLE 03408_dist(id Int32, val String) engine = Distributed(test_cluster_two_shards, currentDatabase(), 03408_local, id);

SELECT '-- Assert total number of groups and records in distributed';
SELECT uniqExact(id), count() FROM 03408_dist;

SELECT '';
SELECT '-- Assert rows_before_limit for distributed ORDER BY + LIMIT BY + LIMIT';

SELECT id, val FROM 03408_dist ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=0;

SELECT '';
SELECT '-- Assert rows_before_limit for distributed ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_dist ORDER BY id, val LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

SELECT '';
SELECT '-- Assert rows_before_limit for distributed HAVING + ORDER BY + LIMIT BY + LIMIT, exact';

SELECT id, val FROM 03408_dist GROUP BY id, val HAVING id < 7 ORDER BY id, val DESC LIMIT 1 BY id LIMIT 3
FORMAT JsonCompact SETTINGS max_block_size=1, exact_rows_before_limit=1;

SELECT '';
SELECT '-- Assert rows_before_limit for distributed without LIMIT BY on initiator, exact';

SELECT id, max(val) FROM 03408_dist GROUP BY id ORDER BY id LIMIT 1 BY id LIMIT 4
FORMAT JSONCompact SETTINGS max_block_size=1, exact_rows_before_limit = 1, distributed_group_by_no_merge=2;

DROP TABLE 03408_local;
DROP TABLE 03408_dist;
