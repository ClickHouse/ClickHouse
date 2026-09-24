-- Tests that a column TTL which empties a column does not leave a stale statistic for it behind:
-- min()/max() and a filtered count() on that column must agree with the rows the table stores.

SET mutations_sync = 2, alter_sync = 2, materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS ttl_stats_orphan;
CREATE TABLE ttl_stats_orphan (d DateTime, v UInt32 DEFAULT 7 STATISTICS(basic) TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

-- these rows arrive already expired, so the TTL empties `v` and drops it from the part
INSERT INTO ttl_stats_orphan SELECT now() - INTERVAL 1 DAY, 5 FROM numbers(4);
ALTER TABLE ttl_stats_orphan MATERIALIZE TTL;
-- changing the default makes a stale statistic distinguishable from a correct read
ALTER TABLE ttl_stats_orphan MODIFY COLUMN v UInt32 DEFAULT 99 STATISTICS(basic) TTL d + INTERVAL 1 SECOND;
-- a second part that really stores a large value
INSERT INTO ttl_stats_orphan SELECT now() + INTERVAL 10 YEAR, 4242 FROM numbers(2);

-- fixture integrity: `v` must be gone from exactly one of the two parts
SELECT count() FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'ttl_stats_orphan' AND active AND column = 'v';

SELECT arraySort(groupArray(v)) FROM ttl_stats_orphan;
SELECT min(v), max(v) FROM ttl_stats_orphan;
SELECT count() FROM ttl_stats_orphan WHERE v > 50;
SELECT count(), sum(v) FROM ttl_stats_orphan;

DROP TABLE ttl_stats_orphan;
