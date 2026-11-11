-- `TTL GROUP BY ... SET` assigns a new value to a column the sorting key is calculated from, so the aggregated rows
-- may become out of order (https://github.com/ClickHouse/ClickHouse/issues/108514). Then the columns the sorting key
-- is calculated from are replaced in such a row with the values of the previous row, and the part stays sorted.
-- All rows are inserted in one part, so that the result does not depend on how many merges happen.
-- With `index_granularity = 1` every row is present in the primary index, which must correspond to the data.
-- The second `OPTIMIZE` merges the written part again: a debug build checks that the part is sorted.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_group_by_set_sorting_key;

CREATE TABLE t_ttl_group_by_set_sorting_key (k Float64, ts DateTime('UTC'), v Float64)
ENGINE = MergeTree ORDER BY (k, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY k, toStartOfDay(ts) SET ts = max(ts) + INTERVAL 100 YEAR, k = max(v)
SETTINGS index_granularity = 1;

INSERT INTO t_ttl_group_by_set_sorting_key VALUES (1.0, '2000-06-09 10:00:00', 96827), (1.0, '2000-06-10 10:00:00', 41302);
OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;

SELECT '-- The second group gets `k` and `ts` of the first one';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY _part_offset;
SELECT '-- The primary index corresponds to the data';
SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ttl_group_by_set_sorting_key) WHERE rows_in_granule > 0 ORDER BY mark_number;

OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;
SELECT count() FROM t_ttl_group_by_set_sorting_key;

DROP TABLE t_ttl_group_by_set_sorting_key;

CREATE TABLE t_ttl_group_by_set_sorting_key (id String, ts DateTime('UTC'), value String)
ENGINE = MergeTree ORDER BY (id, toStartOfDay(ts))
TTL ts + toIntervalDay(1) GROUP BY id, toStartOfDay(ts) SET ts = max(ts) + INTERVAL 100 YEAR, id = max(value)
SETTINGS index_granularity = 1;

INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('pepe', '2000-06-09 10:00:00', 'zzz'), ('pepe', '2000-06-10 10:00:00', 'aaa'), ('pepe', '2000-06-11 10:00:00', 'mmm'), ('pepe', '2050-01-01 00:00:00', 'not expired');
OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;

SELECT '-- Aggregated and non-expired rows after the first group get `id` and `ts` of the first group';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY _part_offset;
SELECT '-- The primary index corresponds to the data';
SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ttl_group_by_set_sorting_key) WHERE rows_in_granule > 0 ORDER BY mark_number;

OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;
SELECT count() FROM t_ttl_group_by_set_sorting_key;

DROP TABLE t_ttl_group_by_set_sorting_key;

-- A mutation calculates only the primary key, so `toStartOfDay(ts)` is not in its header and the algorithm calculates
-- the full sorting key itself. The aggregated row and the non-expired row have the same `id`, so the order is violated
-- only in `toStartOfDay(ts)`. TTL merges are stopped, so that the TTL is executed only by the mutation.

CREATE TABLE t_ttl_group_by_set_sorting_key (id String, ts DateTime('UTC'), value String)
ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, toStartOfDay(ts))
SETTINGS index_granularity = 1;

SYSTEM STOP TTL MERGES t_ttl_group_by_set_sorting_key;

INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('a', '2000-06-09 10:00:00', 'expired'), ('a', '2000-06-10 10:00:00', 'expired too'), ('a', '2050-01-01 00:00:00', 'not expired');
ALTER TABLE t_ttl_group_by_set_sorting_key MODIFY TTL ts + toIntervalDay(1) GROUP BY id SET ts = max(ts) + INTERVAL 100 YEAR;

SELECT '-- The non-expired row gets `ts` of the aggregated row after MATERIALIZE TTL';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY _part_offset;
SELECT '-- The primary index corresponds to the data';
SELECT * EXCEPT part_name FROM mergeTreeIndex(currentDatabase(), t_ttl_group_by_set_sorting_key) WHERE rows_in_granule > 0 ORDER BY mark_number;

SYSTEM START TTL MERGES t_ttl_group_by_set_sorting_key;
OPTIMIZE TABLE t_ttl_group_by_set_sorting_key FINAL;
SELECT count() FROM t_ttl_group_by_set_sorting_key;

DROP TABLE t_ttl_group_by_set_sorting_key;

-- The secondary indices are calculated after the TTL rewrites the data, so they correspond to the new part:
-- `toYYYYMM(ts)` of every row is the one of the aggregated row, not the one of the source part.

CREATE TABLE t_ttl_group_by_set_sorting_key
(
    id String,
    ts DateTime('UTC'),
    value String,
    INDEX idx_month toYYYYMM(ts) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree PRIMARY KEY id ORDER BY (id, toStartOfDay(ts))
SETTINGS index_granularity = 1;

SYSTEM STOP TTL MERGES t_ttl_group_by_set_sorting_key;

INSERT INTO t_ttl_group_by_set_sorting_key VALUES ('a', '2000-06-09 10:00:00', 'expired'), ('a', '2000-06-10 10:00:00', 'expired too'), ('a', '2050-01-01 00:00:00', 'not expired');
ALTER TABLE t_ttl_group_by_set_sorting_key MODIFY TTL ts + toIntervalDay(1) GROUP BY id SET ts = max(ts) + INTERVAL 100 YEAR;

SELECT '-- The data after MATERIALIZE TTL';
SELECT * FROM t_ttl_group_by_set_sorting_key ORDER BY _part_offset;
SELECT '-- The skip index corresponds to the data: the rows are found by the rewritten value and not by the old one';
SELECT count() FROM t_ttl_group_by_set_sorting_key WHERE toYYYYMM(ts) = 210006;
SELECT count() FROM t_ttl_group_by_set_sorting_key WHERE toYYYYMM(ts) = 200006;
SELECT count() FROM t_ttl_group_by_set_sorting_key WHERE toYYYYMM(ts) = 205001;

DROP TABLE t_ttl_group_by_set_sorting_key;
