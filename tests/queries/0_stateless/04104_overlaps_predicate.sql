-- Basic overlapping periods: s1=2020-01-01 < e2=2020-09-01 AND s2=2020-03-01 < e1=2020-06-01 => 1
SELECT (toDate('2020-01-01'), toDate('2020-06-01')) OVERLAPS (toDate('2020-03-01'), toDate('2020-09-01'));
-- Non-overlapping: s1 < e2 but NOT s2 < e1 => 0
SELECT (toDate('2020-01-01'), toDate('2020-03-01')) OVERLAPS (toDate('2020-06-01'), toDate('2020-09-01'));
-- Adjacent periods (touching at 2020-06-01): strict inequality so no overlap => 0
SELECT (toDate('2020-01-01'), toDate('2020-06-01')) OVERLAPS (toDate('2020-06-01'), toDate('2020-09-01'));
-- One period fully contained in the other => 1
SELECT (toDate('2020-01-01'), toDate('2020-12-01')) OVERLAPS (toDate('2020-03-01'), toDate('2020-09-01'));
-- With DateTime
SELECT (toDateTime('2020-01-01 00:00:00'), toDateTime('2020-06-01 00:00:00')) OVERLAPS (toDateTime('2020-03-01 00:00:00'), toDateTime('2020-09-01 00:00:00'));
-- With integers
SELECT (1, 5) OVERLAPS (3, 7);
SELECT (1, 3) OVERLAPS (5, 7);
