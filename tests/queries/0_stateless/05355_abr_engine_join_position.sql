-- An `ABR` table executes the query it is given on a source table, so a JOIN with it would be executed in each attempt, where the
-- other tables of the JOIN would count against the share of the limits of the attempt. So it cannot be joined directly, on any side
-- of a JOIN, and is read through a subquery instead. `ARRAY JOIN` reads no other table, and is allowed.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';
-- With parallel replicas, the right side of a JOIN is read as a query of its own, without the filters of the JOIN, so
-- it is rejected for the missing filter on the time column before the JOIN is checked.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS src_10;
DROP TABLE IF EXISTS lhs;

CREATE TABLE src_1 (id UInt64, ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE src_10 (id UInt64, ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT number, toDateTime('2026-01-01 00:00:00') + INTERVAL number HOUR, 1 FROM numbers(10);
INSERT INTO src_10 SELECT number, toDateTime('2026-01-01 00:00:00') + INTERVAL number HOUR, 10 FROM numbers(2);
CREATE TABLE abr ENGINE = ABR('src', [1, 10], ('ts'));

CREATE TABLE lhs (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lhs SELECT number FROM numbers(25);

SET enable_analyzer = 1;

-- On the right of a JOIN, with and without swapping the sides of the join.
SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00')
SETTINGS query_plan_join_swap_table = 'false'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }

-- On the left of a JOIN.
SELECT count() FROM abr AS r INNER JOIN lhs AS l ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM abr AS r CROSS JOIN lhs AS l WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM abr AS r, lhs AS l WHERE r.ts >= toDateTime('2026-01-01 00:00:00') AND l.id = r.id; -- { serverError NOT_IMPLEMENTED }

-- Through a subquery, on both sides of a JOIN.
SELECT count() FROM lhs AS l INNER JOIN (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r ON l.id = r.id;
SELECT count() FROM (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r INNER JOIN lhs AS l ON l.id = r.id;

-- Through a subquery, the rows of the other table do not count against the share of the limit of an attempt. Each of the two
-- attempts gets 30 rows: the finest source table and `lhs` together read 35 rows, but the finest source table alone reads 10,
-- so the query is not retried on the coarse source table, whose `value` is 10.
SELECT max(r.value), count()
FROM (SELECT id, value FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r INNER JOIN lhs AS l ON l.id = r.id
SETTINGS max_rows_to_read = 60;

-- `ARRAY JOIN` reads no other table.
SELECT count() FROM abr ARRAY JOIN [1, 2] AS x WHERE ts >= toDateTime('2026-01-01 00:00:00');

SET enable_analyzer = 0;

SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM abr AS r INNER JOIN lhs AS l ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM lhs AS l INNER JOIN (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r ON l.id = r.id;
SELECT count() FROM (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r INNER JOIN lhs AS l ON l.id = r.id;
SELECT count() FROM abr ARRAY JOIN [1, 2] AS x WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE src_10;
DROP TABLE lhs;
