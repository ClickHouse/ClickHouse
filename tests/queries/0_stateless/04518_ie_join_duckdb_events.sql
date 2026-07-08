-- Ported from DuckDB test/sql/join/iejoin/test_iejoin_events.test: the interval overlap
-- query Q2 from the IEJoin paper. The original uses seeded `random()`; here the events are
-- derived deterministically and the results are compared with the cross join with a filter.

SET allow_experimental_ie_join = 1;

DROP TABLE IF EXISTS events;

CREATE TABLE events ENGINE = MergeTree ORDER BY id AS
SELECT number + 1 AS id,
       toDateTime('1992-01-01 00:00:00', 'UTC')
           + toIntervalDay(cityHash64(number, 1) % 14600)
           + toIntervalHour(cityHash64(number, 2) % 24) AS s,
       s + toIntervalMinute(if(cityHash64(number, 3) % 10 = 0, 120, 5 + toInt64(cityHash64(number, 4) % 51))) AS e
FROM numbers(999);

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT count() FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s) WHERE explain LIKE '%IEJoin%';

SELECT (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s
) = (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s
    SETTINGS allow_experimental_ie_join = 0
);

-- With the additional `<>` condition of the original query (three conditions fall back,
-- the result must stay correct)
SELECT (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s AND r.id <> s2.id
) = (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s AND r.id <> s2.id
    SETTINGS allow_experimental_ie_join = 0
);

-- Every event overlaps at least itself
SELECT count() >= 999 FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s;

DROP TABLE events;
