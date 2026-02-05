-- { echo ON }

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 DESC) SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(10);

SELECT c0 FROM t0 JOIN t0 tx USING (c0) ORDER BY c0 SETTINGS query_plan_join_shard_by_pk_ranges = 1, max_threads = 2;

DROP TABLE t0;
