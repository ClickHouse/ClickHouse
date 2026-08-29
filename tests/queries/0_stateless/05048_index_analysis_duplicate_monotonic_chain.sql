-- Two condition atoms over one key column carry chains built independently, so index analysis may
-- only share a transformed range between chains that compute the same thing.

DROP TABLE IF EXISTS t_dup_chain_mm;
CREATE TABLE t_dup_chain_mm (ts DateTime, INDEX i_ts (ts) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_dup_chain_mm SELECT toDateTime(1700000000 + number * 37) FROM numbers(4096)
SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_dup_chain_mm FINAL;

-- Equivalent chains.
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfHour(ts, 'UTC') >= toDateTime(1700010000) AND toStartOfHour(ts, 'UTC') <= toDateTime(1700020000);
-- Zones whose day boundaries differ, so the two chains are not interchangeable.
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfDay(ts, 'UTC') >= toDateTime(1700006400) AND toStartOfDay(ts, 'Asia/Tokyo') <= toDateTime(1699974000);
-- Different chain function.
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfHour(ts, 'UTC') >= toDateTime(1700010000) AND toStartOfDay(ts, 'UTC') <= toDateTime(1700006400);
-- The same pair in the opposite order, so neither atom may adopt the other's transformation.
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfDay(ts, 'UTC') <= toDateTime(1700092800) AND toStartOfHour(ts, 'UTC') >= toDateTime(1700010000);
-- Sharing one transformation between equivalent atoms must still prune granules.
SELECT countIf(toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)'))) > 0 FROM
(
    EXPLAIN indexes = 1 SELECT count() FROM t_dup_chain_mm
    WHERE toStartOfHour(ts, 'UTC') >= toDateTime(1700010000) AND toStartOfHour(ts, 'UTC') <= toDateTime(1700020000)
);

-- Two equivalent atoms must apply the chain as many times as one atom does, otherwise the sharing
-- has silently stopped: correct results and no other signal anywhere. The query condition cache is
-- off for both, because a hit skips the granules an identical earlier query already excluded and the
-- two counts would then be measured over different granule sets.
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfHour(ts, 'UTC') >= toDateTime(1700010000) AND toStartOfHour(ts, 'UTC') <= toDateTime(1700020000)
SETTINGS log_comment = '05048_two_equivalent_atoms', use_query_condition_cache = 0;
SELECT count() FROM t_dup_chain_mm
WHERE toStartOfHour(ts, 'UTC') >= toDateTime(1700010000)
SETTINGS log_comment = '05048_one_atom', use_query_condition_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT count() = 2 AND uniqExact(applications) = 1 AND min(applications) > 0 AND uniqExact(marks) = 1
FROM
(
    -- The latest row per comment, so a repeated run against the same database is not confused by
    -- an earlier one.
    SELECT
        argMax(ProfileEvents['IndexMonotonicFunctionChainApplications'], event_time_microseconds) AS applications,
        argMax(ProfileEvents['SelectedMarksTotal'], event_time_microseconds) AS marks
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND log_comment IN ('05048_two_equivalent_atoms', '05048_one_atom')
    GROUP BY log_comment
);

DROP TABLE t_dup_chain_mm;

DROP TABLE IF EXISTS t_dup_chain_pk;
CREATE TABLE t_dup_chain_pk (ts DateTime) ENGINE = MergeTree ORDER BY ts
SETTINGS index_granularity = 8;
INSERT INTO t_dup_chain_pk SELECT toDateTime(1700000000 + number * 37) FROM numbers(4096)
SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_dup_chain_pk FINAL;

-- The same pair over a primary key.
SELECT count() FROM t_dup_chain_pk
WHERE toStartOfDay(ts, 'UTC') >= toDateTime(1700006400) AND toStartOfDay(ts, 'Asia/Tokyo') <= toDateTime(1699974000);

DROP TABLE t_dup_chain_pk;

DROP TABLE IF EXISTS t_dup_chain_cast;
CREATE TABLE t_dup_chain_cast (z UInt64, INDEX i_z (z) TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_dup_chain_cast SELECT number + 65536 FROM numbers(4096) SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_dup_chain_cast FINAL;

-- Different cast target.
SELECT count() FROM t_dup_chain_cast WHERE CAST(z, 'UInt16') >= 100 AND CAST(z, 'UInt8') <= 200;
-- Equivalent casts.
SELECT count() FROM t_dup_chain_cast WHERE CAST(z, 'UInt16') >= 100 AND CAST(z, 'UInt16') <= 300;
-- A constant of equal value but different type is a different chain.
SELECT count() FROM t_dup_chain_cast WHERE (1::UInt8 + z) >= 65600 AND (1::UInt16 + z) <= 65700;

DROP TABLE t_dup_chain_cast;
