-- The insert deduplication hash must not depend on the internal representation of a column. A hash join
-- carries the probe side as a lazily replicated column, whose generic per-row hashing produces a
-- different byte stream than the dense column's range overload, so a retry of the same logical insert
-- with the column in dense form used to insert the rows a second time.

DROP TABLE IF EXISTS t_dedup_probe;
DROP TABLE IF EXISTS t_dedup_build;
DROP TABLE IF EXISTS t_dedup_dst;

CREATE TABLE t_dedup_probe (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dedup_build (y UInt64, n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_dedup_probe SELECT number, concat('payload_longer_than_8_bytes_', toString(number)) FROM numbers(10);
INSERT INTO t_dedup_build SELECT number % 10, number FROM numbers(1000);

CREATE TABLE t_dedup_dst (s String, n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dedup_dst', '1') ORDER BY n;

INSERT INTO t_dedup_dst SELECT p.s AS s, b.n AS n FROM t_dedup_probe p INNER JOIN t_dedup_build b ON p.k = b.y
SETTINGS query_plan_join_swap_table = 'false', max_threads = 1,
         deduplicate_insert_select = 'enable_even_for_bad_queries', enable_lazy_columns_replication = 1;

-- The same rows in the same order, only the column representation differs.
INSERT INTO t_dedup_dst SELECT p.s AS s, b.n AS n FROM t_dedup_probe p INNER JOIN t_dedup_build b ON p.k = b.y
SETTINGS query_plan_join_swap_table = 'false', max_threads = 1,
         deduplicate_insert_select = 'enable_even_for_bad_queries', enable_lazy_columns_replication = 0;

SELECT count() FROM t_dedup_dst;

DROP TABLE t_dedup_probe;
DROP TABLE t_dedup_build;
DROP TABLE t_dedup_dst;
