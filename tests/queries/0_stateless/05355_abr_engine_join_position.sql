-- An `ABR` table executes the query it is given on a source table, so it can only be the leftmost table of a JOIN,
-- which is given the whole join. On another side of a JOIN it is rejected, and can be read through a subquery.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';
-- With parallel replicas, the right side of a JOIN is read as a query of its own, without the filters of the JOIN, so
-- it is rejected for the missing filter on the time column before its position is checked.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;
DROP TABLE IF EXISTS lhs;

CREATE TABLE src_1 (id UInt64, ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT number, toDateTime('2026-01-01 00:00:00') + INTERVAL number HOUR, number FROM numbers(10);
CREATE TABLE abr ENGINE = ABR('src', [1], ('ts'));

CREATE TABLE lhs (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO lhs SELECT number FROM numbers(5);

SET enable_analyzer = 1;

-- On the right of a JOIN, with and without swapping the sides of the join.
SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00')
SETTINGS query_plan_join_swap_table = 'false'; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }

-- On the left of a JOIN.
SELECT count() FROM abr AS r INNER JOIN lhs AS l ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00');

-- On the right of a JOIN, through a subquery.
SELECT count() FROM lhs AS l INNER JOIN (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r ON l.id = r.id;

SET enable_analyzer = 0;

SELECT count() FROM lhs AS l INNER JOIN abr AS r ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00'); -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM abr AS r INNER JOIN lhs AS l ON l.id = r.id WHERE r.ts >= toDateTime('2026-01-01 00:00:00');
SELECT count() FROM lhs AS l INNER JOIN (SELECT id FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00')) AS r ON l.id = r.id;

DROP TABLE abr;
DROP TABLE src_1;
DROP TABLE lhs;
