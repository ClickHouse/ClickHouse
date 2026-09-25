-- Tags: shard

-- A `Distributed` table whose declared column type differs from the type in the shards' table.
-- The shards resolve their own table and sort by its type, and the initiator casts the result
-- to the declared type only afterwards, above the shards' sort and preliminary LIMIT. When the
-- cast does not preserve the order (`String` to `Int8` here: '10' sorts before '2' as a string
-- and after it as a number), the initiator merges the streams as if they were sorted by the
-- declared type: the values come out in the wrong order, LIMIT cuts the wrong ones, and
-- `DistinctSortedStreamTransform` sees equal values that are not adjacent - in a debug build an
-- exception, in a release build duplicates in the DISTINCT result. A shard sorts at every stage
-- above `FetchColumns`, and a `Distributed` table cannot be read at `FetchColumns`, so
-- `StorageDistributed::getQueryProcessingStage` refuses such a query with ORDER BY when the local
-- copy of the shards' table shows the mismatch, instead of returning wrong rows.

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS dist_int8;
DROP TABLE IF EXISTS dist_one_int8;
DROP TABLE IF EXISTS dist_str;
DROP TABLE IF EXISTS t_i32;
DROP TABLE IF EXISTS dist_i64;
DROP TABLE IF EXISTS dist_u32;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS dist_wide;
DROP TABLE IF EXISTS dist_narrow_dec;
DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS dist_mixed;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS dist_arr;

CREATE TABLE t_str (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_str SELECT toString(number % 21) FROM numbers(100);

-- Both shards read the same table, so every value is present twice on the initiator.
CREATE TABLE dist_int8 (s Int8) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_str);
CREATE TABLE dist_one_int8 (s Int8) ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_str);
-- The control with the matching type.
CREATE TABLE dist_str AS t_str ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_str);

SELECT '-- the reported query, and the other shapes that trust the shard order, are refused';
-- `optimize_distinct_in_order` is pinned because the runner randomizes it, and with it 0 the
-- initiator hash-dedups the stream and the count is right even when the order is corrupt.
SELECT count() FROM (SELECT DISTINCT s FROM dist_int8 ORDER BY s ASC LIMIT 100)
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0, optimize_distinct_in_order = 1; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT count() FROM (SELECT DISTINCT ON (s) s FROM dist_int8 ORDER BY s ASC LIMIT 100)
    SETTINGS max_threads = 1, distributed_aggregation_memory_efficient = 0; -- { serverError INCOMPATIBLE_COLUMNS }
-- As strings the three largest values are 9, 8, 7, which is what came back before.
SELECT DISTINCT s FROM dist_int8 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT s FROM dist_int8 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT s FROM dist_int8 ORDER BY s DESC LIMIT 3 SETTINGS prefer_localhost_replica = 0; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT s FROM dist_int8 ORDER BY s DESC LIMIT 3 SETTINGS distributed_push_down_limit = 0; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT s FROM dist_int8 ORDER BY s DESC LIMIT 3 SETTINGS distributed_group_by_no_merge = 2; -- { serverError INCOMPATIBLE_COLUMNS }
-- A single shard is processed to Complete, and the cast lands above that sort as well.
SELECT s FROM dist_one_int8 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- without ORDER BY nothing relies on the shard order, so the queries still work';
SELECT count() FROM dist_int8;
SELECT count() FROM (SELECT DISTINCT s FROM dist_int8);
-- `group_by_two_level_threshold` is pinned: with it 1 the shards send a two-level `String` state that the
-- initiator cannot merge into its single-level `Int8` one (`UNKNOWN_AGGREGATED_DATA_VARIANT`). That is a
-- separate defect of the mismatch, present without this change (https://github.com/ClickHouse/ClickHouse/issues/120781);
-- here only the order is the subject.
SELECT count() FROM (SELECT s, count() FROM dist_int8 GROUP BY s) SETTINGS group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
SELECT count() FROM (SELECT * FROM dist_int8 LIMIT 1 BY s);
-- An ORDER BY outside the query over the table is applied by the initiator to converted values.
SELECT * FROM (SELECT DISTINCT s FROM dist_int8) ORDER BY s DESC LIMIT 3;

SELECT '-- the control with the matching type merges the sorted shard streams';
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT s FROM dist_str ORDER BY s) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT s FROM dist_str ORDER BY s DESC LIMIT 3;

SELECT '-- a differing but order-preserving type is accepted, and the values are right';
-- Refusing these would break tables that are correct today.
CREATE TABLE t_i32 (A Int32) ENGINE = MergeTree ORDER BY A;
INSERT INTO t_i32 SELECT -number FROM numbers(1000);
CREATE TABLE dist_i64 (A Int64) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_i32);
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT A FROM dist_i64 ORDER BY A) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT A FROM dist_i64 ORDER BY A ASC LIMIT 3;

SELECT '-- exact widenings are accepted: Float32/Float64, Date/Date32, DateTime/DateTime64, Decimal, FixedString/String';
CREATE TABLE t_wide (f Float32, d Date, dt DateTime('UTC'), dec Decimal(9, 2), fs FixedString(3)) ENGINE = MergeTree ORDER BY f;
INSERT INTO t_wide SELECT number / 4, toDate('2020-01-01') + number, toDateTime('2020-01-01 00:00:00', 'UTC') + number, number / 4, toFixedString(leftPad(toString(number), 3, '0'), 3) FROM numbers(50);
CREATE TABLE dist_wide (f Float64, d Date32, dt DateTime64(3, 'UTC'), dec Decimal(18, 4), fs String)
    ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_wide);
SELECT count() > 0 FROM (EXPLAIN PLAN sorting = 1 SELECT DISTINCT f FROM dist_wide ORDER BY f) WHERE explain ILIKE '%Merge sorted streams%';
SELECT DISTINCT f FROM dist_wide ORDER BY f DESC LIMIT 2;
SELECT DISTINCT d FROM dist_wide ORDER BY d DESC LIMIT 2;
SELECT DISTINCT dt FROM dist_wide ORDER BY dt DESC LIMIT 2;
SELECT DISTINCT dec FROM dist_wide ORDER BY dec DESC LIMIT 2;
SELECT DISTINCT fs FROM dist_wide ORDER BY fs DESC LIMIT 2;

SELECT '-- only the columns the query sorts by are checked';
-- A table with one sloppily declared column must not lose the ORDER BY queries that never read it:
-- a column no sorting key expression reads is converted above the shards' sort like any other
-- expression and its values are carried along in whatever order the sorted columns dictate.
CREATE TABLE t_mixed (k Int32, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_mixed SELECT number, toString(number % 21) FROM numbers(100);
CREATE TABLE dist_mixed (k Int32, s Int8) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_mixed);
SELECT k FROM dist_mixed ORDER BY k DESC LIMIT 2;
-- The mismatched column can be selected as long as it is not sorted by.
SELECT s FROM dist_mixed ORDER BY k DESC LIMIT 2;
-- Sorting by it is still refused, whether it is selected or not, and through an expression over it.
SELECT s FROM dist_mixed ORDER BY s DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT k FROM dist_mixed ORDER BY s DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT k FROM dist_mixed ORDER BY -s DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT k FROM dist_mixed ORDER BY k, s LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
-- An alias of the column, and an alias of an expression over it, are resolved back to it.
SELECT s AS x FROM dist_mixed ORDER BY x LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT -s AS x FROM dist_mixed ORDER BY x LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- a sorting key that reads no column of the table does not rely on the shard order';
SELECT count() FROM (SELECT s FROM dist_mixed ORDER BY tuple() LIMIT 2);
SELECT count() FROM (SELECT s FROM dist_mixed ORDER BY 1 + 1 LIMIT 2);
SELECT count() FROM (SELECT s FROM dist_mixed ORDER BY 'x' LIMIT 2);
-- A positional argument names the column it stands for.
SELECT s FROM dist_mixed ORDER BY 1 DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT k FROM dist_mixed ORDER BY 1 DESC LIMIT 2;
SELECT k FROM dist_mixed ORDER BY ALL LIMIT 2;
SELECT s FROM dist_mixed ORDER BY ALL LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- a column of another table or scope with the same name is not this table''s column';
CREATE TABLE dim (k Int32, s Int8) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim SELECT number, number % 7 FROM numbers(100);
-- `dim.s` is a real `Int8` on the shards and the initiator alike; `dist_mixed.s` is not sorted by.
SELECT dist_mixed.k, dim.s FROM dist_mixed JOIN dim ON dist_mixed.k = dim.k ORDER BY dim.s DESC, dist_mixed.k DESC LIMIT 2 SETTINGS distributed_product_mode = 'local';
SELECT dist_mixed.k, dim.s FROM dist_mixed GLOBAL JOIN dim ON dist_mixed.k = dim.k ORDER BY dim.s DESC, dist_mixed.k DESC LIMIT 2;
-- ... while sorting by this table's `s` through the join stays refused.
SELECT dist_mixed.k FROM dist_mixed JOIN dim ON dist_mixed.k = dim.k ORDER BY dist_mixed.s DESC LIMIT 2 SETTINGS distributed_product_mode = 'local'; -- { serverError INCOMPATIBLE_COLUMNS }
-- A `JOIN USING` key is traced to the columns of both sides: `k` does not read the retyped `s` ...
SELECT k FROM dist_mixed JOIN dim USING (k) ORDER BY k DESC LIMIT 2 SETTINGS distributed_product_mode = 'local';
-- ... and `s` reads it through the left side.
SELECT s FROM dist_mixed JOIN dim USING (s) ORDER BY s DESC LIMIT 2 SETTINGS distributed_product_mode = 'local'; -- { serverError INCOMPATIBLE_COLUMNS }
-- A lambda argument named like the column is a value of the array, not the column.
SELECT k FROM dist_mixed ORDER BY arrayMap(s -> s + 1, [k]) DESC LIMIT 2;
-- A subquery over the table sorted by an unrelated column, with an outer sort by the retyped one, is
-- sorted by the initiator over converted values.
SELECT s FROM (SELECT k, s FROM dist_mixed ORDER BY k LIMIT 10) ORDER BY s DESC LIMIT 2;

SELECT '-- an ARRAY JOIN column is traced back to its array';
CREATE TABLE t_arr (k Int32, arr Array(String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_arr SELECT number, [toString(number % 21), toString(number % 13)] FROM numbers(100);
CREATE TABLE dist_arr (k Int32, arr Array(Int8)) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_arr);
SELECT a FROM dist_arr ARRAY JOIN arr AS a ORDER BY a DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT arr FROM dist_arr ARRAY JOIN arr ORDER BY arr DESC LIMIT 2; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT k FROM dist_arr ARRAY JOIN arr AS a ORDER BY k DESC LIMIT 2;
SELECT a FROM dist_arr ARRAY JOIN [1, 2] AS a ORDER BY a DESC LIMIT 2;

SELECT '-- a smaller decimal scale rounds distinct values together, so it is refused';
CREATE TABLE dist_narrow_dec (dec Decimal(18, 1)) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_wide);
SELECT DISTINCT dec FROM dist_narrow_dec ORDER BY dec LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT count() FROM (SELECT DISTINCT dec FROM dist_narrow_dec);

SELECT '-- an equal-width sign change does not preserve the order';
CREATE TABLE dist_u32 (A UInt32) ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_i32);
SELECT DISTINCT A FROM dist_u32 ORDER BY A ASC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT count() FROM (SELECT DISTINCT A FROM dist_u32);

DROP TABLE dist_u32;
DROP TABLE dist_arr;
DROP TABLE t_arr;
DROP TABLE dim;
DROP TABLE dist_mixed;
DROP TABLE t_mixed;
DROP TABLE dist_narrow_dec;
DROP TABLE dist_wide;
DROP TABLE t_wide;
DROP TABLE dist_i64;
DROP TABLE t_i32;
DROP TABLE dist_str;
DROP TABLE dist_one_int8;
DROP TABLE dist_int8;
DROP TABLE t_str;
