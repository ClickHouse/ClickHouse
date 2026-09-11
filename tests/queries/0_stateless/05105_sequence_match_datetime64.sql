-- `sequenceMatch`, `sequenceCount` and `sequenceMatchEvents` accept a `DateTime64` timestamp.
-- Durations in the `(?t...)` conditions of the pattern are seconds for every timestamp type,
-- so the same pattern keeps its meaning when a column changes precision.

SELECT 'the query from the issue';
DROP TABLE IF EXISTS t_seq_dt64;
CREATE TABLE t_seq_dt64 (test_field Nullable(Int64), test_key Nullable(String), test_timestamp Nullable(DateTime64(6)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_seq_dt64 VALUES (1000, 'k', '2021-01-01 00:00:00.000000'), (2000, 'k', '2021-01-02 00:00:00.000000'), (1000, 'l', '2021-01-01 00:00:00.000000'), (2000, 'l', '2021-02-01 00:00:00.000000');
SELECT test_key FROM t_seq_dt64 GROUP BY test_key
HAVING sequenceMatch('(?1)(?t<1296000)(?2)')(test_timestamp, test_field = 1000, test_field = 2000)
ORDER BY test_key;
DROP TABLE t_seq_dt64;

SELECT 'durations are seconds at every scale';
-- `toTypeName(any(ts))` rather than `toTypeName(ts)` with a `GROUP BY`: the type is only wanted as a
-- label, and referring to the column outside an aggregate needs a `GROUP BY` that the old analyzer
-- does not accept as covering `ts`.
SELECT
    toTypeName(any(ts)) AS type,
    sequenceMatch('(?1)(?t<10)(?2)')(ts, e = 1, e = 2) AS within_10s,
    sequenceMatch('(?1)(?t<5)(?2)')(ts, e = 1, e = 2) AS within_5s
FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00', 0), 1), (toDateTime64('2020-01-01 00:00:07', 0), 2)]) AS x, x.1 AS ts, x.2 AS e);
SELECT
    toTypeName(any(ts)) AS type,
    sequenceMatch('(?1)(?t<10)(?2)')(ts, e = 1, e = 2) AS within_10s,
    sequenceMatch('(?1)(?t<5)(?2)')(ts, e = 1, e = 2) AS within_5s
FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00', 6), 1), (toDateTime64('2020-01-01 00:00:07', 6), 2)]) AS x, x.1 AS ts, x.2 AS e);
SELECT
    toTypeName(any(ts)) AS type,
    sequenceMatch('(?1)(?t<10)(?2)')(ts, e = 1, e = 2) AS within_10s,
    sequenceMatch('(?1)(?t<5)(?2)')(ts, e = 1, e = 2) AS within_5s
FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00', 9), 1), (toDateTime64('2020-01-01 00:00:07', 9), 2)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'the same answer as DateTime for a whole-second pattern';
SELECT
    sequenceMatch('(?1)(?t<=7)(?2)')(toDateTime(ts), e = 1, e = 2) AS as_datetime,
    sequenceMatch('(?1)(?t<=7)(?2)')(toDateTime64(ts, 3), e = 1, e = 2) AS as_datetime64,
    sequenceCount('(?1)(?t<=7)(?2)')(toDateTime(ts), e = 1, e = 2) AS count_as_datetime,
    sequenceCount('(?1)(?t<=7)(?2)')(toDateTime64(ts, 3), e = 1, e = 2) AS count_as_datetime64
FROM (SELECT arrayJoin([(toDateTime('2020-01-01 00:00:00'), 1), (toDateTime('2020-01-01 00:00:07'), 2)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'sub-second ordering is preserved';
-- The rows are given out of order and differ only below the second, so the match depends on the
-- sub-second part being kept rather than truncated to seconds.
SELECT
    sequenceMatch('(?1)(?2)')(ts, e = 1, e = 2) AS one_then_two,
    sequenceMatch('(?1)(?2)')(ts, e = 2, e = 1) AS two_then_one
FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00.900', 3), 2), (toDateTime64('2020-01-01 00:00:00.100', 3), 1)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'sequenceMatchEvents returns DateTime64 with the sub-second part';
SELECT toTypeName(events), events
FROM
(
    SELECT sequenceMatchEvents('(?1)(?2)')(ts, e = 1, e = 2) AS events
    FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00.125', 3), 1), (toDateTime64('2020-01-01 00:00:03.250', 3), 2)]) AS x, x.1 AS ts, x.2 AS e)
);

SELECT 'a duration spanning the epoch is compared as a signed value';
-- The first event is before 1970 and the second after it, and the gap is 21 seconds. In unsigned
-- arithmetic `base + duration` wraps around zero and the 5-second condition would match.
SELECT
    sequenceMatch('(?1)(?t<=5)(?2)')(ts, e = 1, e = 2) AS within_5s,
    sequenceMatch('(?1)(?t>5)(?2)')(ts, e = 1, e = 2) AS after_5s,
    sequenceMatch('(?1)(?t<=21)(?2)')(ts, e = 1, e = 2) AS within_21s
FROM (SELECT arrayJoin([(toDateTime64('1969-12-31 23:59:40', 3, 'UTC'), 1), (toDateTime64('1970-01-01 00:00:01', 3, 'UTC'), 2)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'a duration larger than the ticks of the timestamp type is still evaluated';
-- 10000000000 seconds do not fit into the ticks of `DateTime64(9)`, but the condition is still meaningful:
-- the two events are 7 seconds apart, so the `<` condition holds and the `>` one does not.
SELECT
    sequenceMatch('(?1)(?t<10000000000)(?2)')(ts, e = 1, e = 2) AS within,
    sequenceMatch('(?1)(?t>10000000000)(?2)')(ts, e = 1, e = 2) AS after
FROM (SELECT arrayJoin([(toDateTime64('2020-01-01 00:00:00', 9), 1), (toDateTime64('2020-01-01 00:00:07', 9), 2)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'timestamps at the boundary of the type do not overflow';
-- The distance between the two timestamps does not fit into `Int64`, so adding the duration to the base
-- timestamp would overflow; the distance itself is compared instead.
SELECT
    sequenceMatch('(?1)(?t<=5)(?2)')(ts, e = 1, e = 2) AS within_5s,
    sequenceMatch('(?1)(?t>5)(?2)')(ts, e = 1, e = 2) AS after_5s
FROM (SELECT arrayJoin([(CAST(-9223372036854775807, 'DateTime64(0, \'UTC\')'), 1), (CAST(9223372036854775807, 'DateTime64(0, \'UTC\')'), 2)]) AS x, x.1 AS ts, x.2 AS e);

SELECT 'other types are still rejected';
SELECT sequenceMatch('(?1)(?2)')('a', 1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
