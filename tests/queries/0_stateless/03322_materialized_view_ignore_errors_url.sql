DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x int) ORDER BY ();

CREATE TABLE dst (x int) AS url('https://127.0.0.1/', JSONEachRow, 'x int');

CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src;

INSERT INTO src SETTINGS materialized_views_ignore_errors = 0 VALUES (1); -- { serverError POCO_EXCEPTION }

INSERT INTO src SETTINGS materialized_views_ignore_errors = 1 VALUES (2);

SELECT * FROM src;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE mv;