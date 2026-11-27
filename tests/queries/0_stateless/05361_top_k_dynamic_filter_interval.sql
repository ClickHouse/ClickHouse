-- Random settings limits: use_top_k_dynamic_filtering=(1, 1); query_plan_max_limit_for_top_k_optimization=(1000, None); max_block_size=(None, 8192)

-- `ORDER BY ... LIMIT` over a column whose type is or contains an `Interval` type, with the top-K dynamic filter.

SET use_top_k_dynamic_filtering = 1;

DROP TABLE IF EXISTS t_interval;
DROP TABLE IF EXISTS t_interval_key;
DROP TABLE IF EXISTS t_interval_minmax;
DROP TABLE IF EXISTS t_interval_memory;
DROP TABLE IF EXISTS t_interval_lc;

CREATE TABLE t_interval
(
    id UInt64,
    d_ns IntervalNanosecond,
    d_us IntervalMicrosecond,
    d_ms IntervalMillisecond,
    d_s IntervalSecond,
    d_min IntervalMinute,
    d_h IntervalHour,
    d_d IntervalDay,
    d_w IntervalWeek,
    d_mon IntervalMonth,
    d_q IntervalQuarter,
    d_y IntervalYear,
    n_s Nullable(IntervalSecond),
    n_ns Nullable(IntervalNanosecond),
    tup Tuple(IntervalHour, IntervalMinute),
    arr Array(IntervalSecond)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_interval SELECT
    number,
    toIntervalNanosecond(number), toIntervalMicrosecond(number), toIntervalMillisecond(number), toIntervalSecond(number),
    toIntervalMinute(number), toIntervalHour(number), toIntervalDay(number), toIntervalWeek(number),
    toIntervalMonth(number), toIntervalQuarter(number), toIntervalYear(number),
    if(number % 50000 = 7, NULL, toIntervalSecond(number)),
    if(number % 50000 = 7, NULL, toIntervalNanosecond(number)),
    (toIntervalHour(intDiv(number, 60)), toIntervalMinute(number % 60)),
    [toIntervalSecond(number)]
FROM numbers(100000);

SELECT d_ns FROM t_interval ORDER BY d_ns LIMIT 3;
SELECT d_ns FROM t_interval ORDER BY d_ns DESC LIMIT 3;
SELECT d_us FROM t_interval ORDER BY d_us LIMIT 3;
SELECT d_us FROM t_interval ORDER BY d_us DESC LIMIT 3;
SELECT d_ms FROM t_interval ORDER BY d_ms LIMIT 3;
SELECT d_ms FROM t_interval ORDER BY d_ms DESC LIMIT 3;
SELECT d_s FROM t_interval ORDER BY d_s LIMIT 3;
SELECT d_s FROM t_interval ORDER BY d_s DESC LIMIT 3;
SELECT d_min FROM t_interval ORDER BY d_min LIMIT 3;
SELECT d_min FROM t_interval ORDER BY d_min DESC LIMIT 3;
SELECT d_h FROM t_interval ORDER BY d_h LIMIT 3;
SELECT d_h FROM t_interval ORDER BY d_h DESC LIMIT 3;
SELECT d_d FROM t_interval ORDER BY d_d LIMIT 3;
SELECT d_d FROM t_interval ORDER BY d_d DESC LIMIT 3;
SELECT d_w FROM t_interval ORDER BY d_w LIMIT 3;
SELECT d_w FROM t_interval ORDER BY d_w DESC LIMIT 3;
SELECT d_mon FROM t_interval ORDER BY d_mon LIMIT 3;
SELECT d_mon FROM t_interval ORDER BY d_mon DESC LIMIT 3;
SELECT d_q FROM t_interval ORDER BY d_q LIMIT 3;
SELECT d_q FROM t_interval ORDER BY d_q DESC LIMIT 3;
SELECT d_y FROM t_interval ORDER BY d_y LIMIT 3;
SELECT d_y FROM t_interval ORDER BY d_y DESC LIMIT 3;

SELECT n_s FROM t_interval ORDER BY n_s ASC NULLS FIRST LIMIT 3;
SELECT n_s FROM t_interval ORDER BY n_s ASC NULLS LAST LIMIT 3;
SELECT n_s FROM t_interval ORDER BY n_s DESC NULLS FIRST LIMIT 3;
SELECT n_s FROM t_interval ORDER BY n_s DESC NULLS LAST LIMIT 3;
SELECT n_ns FROM t_interval ORDER BY n_ns ASC NULLS FIRST LIMIT 3;
SELECT n_ns FROM t_interval ORDER BY n_ns ASC NULLS LAST LIMIT 3;
SELECT n_ns FROM t_interval ORDER BY n_ns DESC NULLS FIRST LIMIT 3;
SELECT n_ns FROM t_interval ORDER BY n_ns DESC NULLS LAST LIMIT 3;

SELECT tup FROM t_interval ORDER BY tup LIMIT 3;
SELECT tup FROM t_interval ORDER BY tup DESC LIMIT 3;

CREATE TABLE t_interval_key (d IntervalSecond) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_interval_key SELECT toIntervalSecond(number) FROM numbers(100000);
SELECT d FROM t_interval_key ORDER BY d DESC LIMIT 3 SETTINGS optimize_read_in_order = 0;

CREATE TABLE t_interval_minmax (id UInt64, d IntervalSecond, INDEX idx d TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_interval_minmax SELECT number, toIntervalSecond(number) FROM numbers(100000);
SELECT d FROM t_interval_minmax ORDER BY d DESC LIMIT 3 SETTINGS use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;

CREATE TABLE t_interval_memory (d IntervalSecond) ENGINE = Memory;
INSERT INTO t_interval_memory SELECT toIntervalSecond(number) FROM numbers(100000);
SELECT d FROM t_interval_memory ORDER BY d LIMIT 3;
SELECT d FROM t_interval_memory ORDER BY d DESC LIMIT 3;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_interval_lc (id UInt64, d LowCardinality(IntervalSecond), n LowCardinality(Nullable(IntervalSecond)))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_interval_lc SELECT number, toIntervalSecond(number), if(number % 50000 = 7, NULL, toIntervalSecond(number))
FROM numbers(100000);
SELECT d FROM t_interval_lc ORDER BY d LIMIT 3;
SELECT d FROM t_interval_lc ORDER BY d DESC LIMIT 3;
SELECT n FROM t_interval_lc ORDER BY n ASC NULLS FIRST LIMIT 3;
SELECT n FROM t_interval_lc ORDER BY n DESC NULLS LAST LIMIT 3;

DROP TABLE t_interval;
DROP TABLE t_interval_key;
DROP TABLE t_interval_minmax;
DROP TABLE t_interval_memory;
DROP TABLE t_interval_lc;
