-- `SAMPLE` is a modifier of one table expression: `t1 SAMPLE 1/2 JOIN t2` samples `t1` only, not `t2`.

DROP TABLE IF EXISTS t_sample_left;
DROP TABLE IF EXISTS t_sample_right;
DROP TABLE IF EXISTS t_sample_third;
DROP TABLE IF EXISTS t_merge_no_sampling_key;
DROP TABLE IF EXISTS t_no_sampling_key;
DROP TABLE IF EXISTS t_merge_left;
DROP TABLE IF EXISTS t_merge_right;

CREATE TABLE t_sample_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, intHash32(v)) SAMPLE BY intHash32(v);
CREATE TABLE t_sample_right (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY (k, intHash32(w)) SAMPLE BY intHash32(w);
CREATE TABLE t_sample_third (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY (k, intHash32(x)) SAMPLE BY intHash32(x);
CREATE TABLE t_no_sampling_key (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_merge_no_sampling_key (k UInt64) ENGINE = Merge(currentDatabase(), '^t_no_sampling_key$');
CREATE TABLE t_merge_left (k UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^t_sample_left$');
CREATE TABLE t_merge_right (k UInt64, w UInt64) ENGINE = Merge(currentDatabase(), '^t_sample_right$');

INSERT INTO t_sample_left SELECT number, number * 10 FROM numbers(4096);
INSERT INTO t_sample_right SELECT number, number * 7 + 3 FROM numbers(4096);
INSERT INTO t_sample_third SELECT number, number * 3 + 1 FROM numbers(4096);
INSERT INTO t_no_sampling_key SELECT number FROM numbers(4096);

-- The same sample taken in a subquery gives the expected count.
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l JOIN t_sample_right AS r ON l.k = r.k;

SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_sample_right AS r ON l.k = r.k JOIN t_sample_right AS r2 ON l.k = r2.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_no_sampling_key AS n ON l.k = n.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_merge_no_sampling_key AS m ON l.k = m.k;
SELECT any(l._sample_factor), any(r._sample_factor) FROM t_sample_left AS l SAMPLE 1/2 JOIN t_sample_right AS r ON l.k = r.k;

SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2 OFFSET 1/2) AS l JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 OFFSET 1/2 JOIN t_sample_right AS r ON l.k = r.k;

-- The outer joins sample the left table only.
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l LEFT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 LEFT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l RIGHT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 RIGHT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l FULL JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 FULL JOIN t_sample_right AS r ON l.k = r.k;

-- A chain of joins samples the first table only.
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l JOIN t_sample_right AS r ON l.k = r.k JOIN t_sample_third AS t ON l.k = t.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_sample_right AS r ON l.k = r.k JOIN t_sample_third AS t ON l.k = t.k;

-- With two `Merge` tables, the `SAMPLE` resolves through the left `Merge` only.
SELECT count() FROM (SELECT k FROM t_merge_left SAMPLE 1/2) AS l JOIN t_merge_right AS r ON l.k = r.k;
SELECT count() FROM t_merge_left AS l SAMPLE 1/2 JOIN t_merge_right AS r ON l.k = r.k;

-- `ARRAY JOIN` on the sampled table multiplies its rows, and the joined table is not sampled.
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l ARRAY JOIN [1, 2, 3] AS e JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 ARRAY JOIN [1, 2, 3] AS e JOIN t_sample_right AS r ON l.k = r.k;

-- A table expression with its own `SAMPLE` is still sampled.
SELECT count() FROM (SELECT k FROM t_sample_left SAMPLE 1/2) AS l JOIN (SELECT k FROM t_sample_right SAMPLE 1/2) AS r ON l.k = r.k;
SELECT count() FROM t_sample_left AS l SAMPLE 1/2 JOIN t_sample_right AS r SAMPLE 1/2 ON l.k = r.k;

-- `GLOBAL RIGHT JOIN` and `GLOBAL FULL JOIN` over two shards swap the sides of the join, and the sample stays on the sharded table.
SET enable_parallel_replicas = 0, max_parallel_replicas = 1, prefer_localhost_replica = 1;
SELECT count() FROM (SELECT k FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) SAMPLE 1/2) AS l RIGHT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) AS l SAMPLE 1/2 GLOBAL RIGHT JOIN t_sample_right AS r ON l.k = r.k;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) AS l SAMPLE 1/2 GLOBAL FULL JOIN t_sample_right AS r ON l.k = r.k;

-- The side-swapped global joins do not apply the `SAMPLE` to a right table without a sampling key.
SELECT count() FROM (SELECT k FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) SAMPLE 1/2) AS l RIGHT JOIN t_no_sampling_key AS n ON l.k = n.k;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) AS l SAMPLE 1/2 GLOBAL RIGHT JOIN t_no_sampling_key AS n ON l.k = n.k;
SELECT count() FROM (SELECT k FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) SAMPLE 1/2) AS l FULL JOIN t_no_sampling_key AS n ON l.k = n.k;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) AS l SAMPLE 1/2 GLOBAL FULL JOIN t_no_sampling_key AS n ON l.k = n.k;
SELECT count() FROM (SELECT k FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) SAMPLE 1/2) AS l RIGHT JOIN t_merge_no_sampling_key AS m ON l.k = m.k;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_sample_left) AS l SAMPLE 1/2 GLOBAL RIGHT JOIN t_merge_no_sampling_key AS m ON l.k = m.k;

DROP TABLE t_sample_left;
DROP TABLE t_sample_right;
DROP TABLE t_sample_third;
DROP TABLE t_merge_no_sampling_key;
DROP TABLE t_no_sampling_key;
DROP TABLE t_merge_left;
DROP TABLE t_merge_right;
