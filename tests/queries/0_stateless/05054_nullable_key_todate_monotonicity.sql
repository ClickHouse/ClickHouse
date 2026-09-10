-- The monotonicity of a conversion does not depend on the `Nullable` wrapper of its argument, so a
-- `toDate`/`toDateTime` predicate over a `Nullable` key must still be usable for index analysis.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_nullable_key;
CREATE TABLE t_nullable_key (x Nullable(DateTime64(6)), y Int64)
ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 4, allow_nullable_key = 1;

INSERT INTO t_nullable_key
SELECT if(number % 7 = 0, NULL, toDateTime64('2026-03-01 00:00:00', 6) + INTERVAL number * 6 HOUR), number
FROM numbers(40);

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_nullable_key WHERE toDate(x) = toDate('2026-03-02')
) WHERE explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_nullable_key WHERE toDate32(x) = toDate32('2026-03-02')
) WHERE explain LIKE '%Granules:%';

SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1 SELECT count() FROM t_nullable_key WHERE toDateTime(x) = toDateTime('2026-03-02 06:00:00')
) WHERE explain LIKE '%Granules:%';

-- The rows the index analysis keeps must be the rows the predicate selects, `NULL`s included.
SELECT count(), sum(y) FROM t_nullable_key WHERE toDate(x) = toDate('2026-03-02');
SELECT count(), sum(y) FROM t_nullable_key WHERE toDate(x) < toDate('2026-03-02');
SELECT count(), sum(y) FROM t_nullable_key WHERE toDate(x) IS NULL;
SELECT count(), sum(y) FROM t_nullable_key WHERE toDate(x) IN (toDate('2026-03-02'), toDate('2026-03-08'));
SELECT count(), sum(y) FROM t_nullable_key WHERE isNull(x) OR toDate(x) = toDate('2026-03-02');

DROP TABLE t_nullable_key;
