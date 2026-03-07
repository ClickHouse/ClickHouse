-- Regression test: TTL with WHERE ... IN (subquery) during vertical merge
-- should not throw "Not-ready Set" exception.
-- The bug was that TTLDeleteFilterStep built expressions with Set objects in a "probe"
-- transform, but then transformPipeline() created new transforms with different Set objects
-- that were never filled by CreatingSetsStep.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=96573&sha=893ca480970c279b42a296d63347c407cb1ba0df&name_0=PR&name_1=Stateless%20tests%20%28amd_debug%2C%20parallel%29

DROP TABLE IF EXISTS t_ttl_in_subquery;

-- Extra column 'b' ensures there is a gathering column for vertical merge.
-- min_bytes_for_wide_part = 0 forces wide parts so vertical merge is chosen.
CREATE TABLE t_ttl_in_subquery (a UInt32, b String, timestamp DateTime)
ENGINE = MergeTree ORDER BY a
TTL timestamp + INTERVAL 1 SECOND WHERE a IN (SELECT number FROM system.numbers LIMIT 10)
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1, min_bytes_for_wide_part = 0;

-- Insert data with timestamp in the past so TTL is expired
INSERT INTO t_ttl_in_subquery SELECT number, 'x', now() - 5 FROM numbers(100);
INSERT INTO t_ttl_in_subquery SELECT number + 100, 'x', now() - 5 FROM numbers(100);

-- Force merge which should trigger vertical TTL delete with IN subquery evaluation
OPTIMIZE TABLE t_ttl_in_subquery FINAL;

-- Rows matching TTL WHERE condition (a < 10) should be deleted, others should remain
SELECT count() FROM t_ttl_in_subquery WHERE a < 10;
SELECT count() > 0 FROM t_ttl_in_subquery WHERE a >= 10;

DROP TABLE t_ttl_in_subquery;
