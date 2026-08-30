-- Tags: no-parallel-replicas
-- no-parallel-replicas: the granule assertions describe the local `MergeTree` read, which parallel
-- replicas replace, and the `RIGHT JOIN` shapes below hit the unrelated logical error of
-- https://github.com/ClickHouse/ClickHouse/issues/113292 there.

-- The cross-type equi-key substitution of the `RIGHT JOIN` pushdown replaces the `USING` key with
-- `CAST(<opposite key>, supertype)`. For a `Date` key joined to a `DateTime` key that conversion is
-- time-zone-dependent: a `Date` becomes midnight of the effective time zone. The pushed-down
-- predicate must evaluate it exactly like the `JOIN` output column does, or the left input would be
-- pruned on the wrong range and the matched row below would come back with a defaulted left side.
-- `Pacific/Apia` is 13-14 hours away from UTC, so a predicate cast that disagreed with the `JOIN`
-- cast could not land on the same midnights.
--
-- The `Date` range stays below 2106: past the `DateTime` overflow the statistics-based part pruning
-- discards the part wrongly, which is the unrelated defect of
-- https://github.com/ClickHouse/ClickHouse/issues/111759.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters = 0;
SET session_timezone = 'Pacific/Apia';

DROP TABLE IF EXISTS mt_date;
CREATE TABLE mt_date (k Date) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 256, index_granularity_bytes = '10Mi';
INSERT INTO mt_date SELECT toDate('2020-01-01') + number FROM numbers(2000);

SELECT 'RIGHT JOIN USING, Date / DateTime keys, session_timezone: left MergeTree prunes granules';
SELECT count() > 0 FROM (
    EXPLAIN PLAN indexes = 1
    SELECT k FROM mt_date AS l RIGHT JOIN (SELECT toDateTime('2023-06-01 00:00:00') AS k) AS r USING (k)
    WHERE k = toDateTime('2023-06-01 00:00:00')
) WHERE explain LIKE '%Granules: 1/%';

-- `l.k` pins that the row is truly matched: a wrongly pruned left input would still return the right
-- row, but with the defaulted left side.
SELECT 'RIGHT JOIN USING, Date / DateTime keys, session_timezone: the row stays matched';
SELECT k, l.k FROM mt_date AS l RIGHT JOIN (SELECT toDateTime('2023-06-01 00:00:00') AS k) AS r USING (k)
WHERE k = toDateTime('2023-06-01 00:00:00');

-- A right row at a non-midnight instant matches no left `Date`: it must survive as an unmatched row,
-- however the left input was pruned.
SELECT 'RIGHT JOIN USING, Date / DateTime keys, session_timezone: unmatched right row preserved';
SELECT k FROM mt_date AS l RIGHT JOIN (SELECT toDateTime('2023-06-01 12:34:56') AS k) AS r USING (k)
WHERE k >= toDateTime('2023-06-01 00:00:00');

DROP TABLE mt_date;
