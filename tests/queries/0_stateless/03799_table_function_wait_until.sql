-- Tags: no-parallel

SELECT * FROM waitUntil(true);

SELECT * FROM waitUntil(false, 3);

SELECT * FROM waitUntil(5, 10);

SELECT * FROM waitUntil(0, 2, 1.5);

SELECT * FROM waitUntil('2025-12-22 19:00:00' + interval 1 hour > now(), 3);

SELECT * FROM waitUntil('2025-12-22 19:00:00' + interval 1 hour > now(), 100); -- { serverError BAD_ARGUMENTS }

SELECT * FROM waitUntil('2025-12-22 19:00:00' + interval 1 hour > now(), 3, 100); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t (n Int8) ENGINE=MergeTree ORDER BY n;

SELECT * FROM waitUntil((SELECT count() > 9 FROM t), 3);

INSERT INTO t SELECT * FROM numbers(10);

SELECT * FROM waitUntil((SELECT count() > 9 FROM t), 3);
