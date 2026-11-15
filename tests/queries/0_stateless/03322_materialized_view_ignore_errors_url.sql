DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x int) ORDER BY ();

CREATE TABLE dst (x int) AS url('http://127.0.0.1/', JSONEachRow, 'x int');

CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src;

INSERT INTO src SETTINGS materialized_views_ignore_errors = 0 VALUES (1); -- { serverError POCO_EXCEPTION }

INSERT INTO src SETTINGS materialized_views_ignore_errors = 1 VALUES (2);

--- value 2 should be in src, value 1 could be in src
SELECT * FROM src WHERE x = 2;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE mv;

-- Ensure this still fails
insert into function url('http://127.0.0.1/foo.tsv', 'TabSeparated', 'key Int') settings http_max_tries=1, materialized_views_ignore_errors=1 values (2); -- { serverError POCO_EXCEPTION }
