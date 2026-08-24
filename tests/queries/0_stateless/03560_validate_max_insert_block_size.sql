-- Test for https://github.com/ClickHouse/ClickHouse/issues/83620

DROP TABLE IF EXISTS t;

CREATE TABLE t (n Int) ENGINE = MergeTree ORDER BY n SETTINGS merge_max_block_size = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t (n Int) ENGINE = MergeTree ORDER BY n SETTINGS merge_max_block_size = 1;
ALTER TABLE t MODIFY SETTING merge_max_block_size = 0; -- { serverError BAD_ARGUMENTS }
INSERT INTO TABLE t (n) SETTINGS max_insert_block_size = 0 VALUES (1); -- { clientError BAD_ARGUMENTS }

DROP TABLE t;
