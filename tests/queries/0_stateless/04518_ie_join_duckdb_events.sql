-- Tags: no-old-analyzer

-- Interval overlap self-join, query Q2 from the IEJoin paper (Khayyat et al., PVLDB 8(13)),
-- over deterministically derived events; results are verified against the same query with
-- IEJoin disabled.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

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
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- With an additional `<>` condition (IEJoin still applies, the extra condition becomes a filter)
SELECT (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s AND r.id <> s2.id
) = (
    SELECT (count(), sum(cityHash64(r.id, s2.id))) FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s AND r.id <> s2.id
    SETTINGS join_algorithm = 'direct,parallel_hash,hash'
);

-- Every event overlaps at least itself
SELECT count() >= 999 FROM events r JOIN events s2 ON r.s <= s2.e AND r.e >= s2.s;

DROP TABLE events;
