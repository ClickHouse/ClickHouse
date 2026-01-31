SET enable_analyzer = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT REGEXP_REPLACE(explain, '_temporary_and_external_tables._tmp_\\w+\\-\\w+\\-\\w+\\-\\w+\\-\\w+', '_temporary_and_external_tables._tmp_UNIQ_ID') FROM
(
    EXPLAIN QUERY TREE
    WITH
    a AS MATERIALIZED (SELECT * FROM users),
    (x -> x) as b
    SELECT name FROM a
);

SELECT REGEXP_REPLACE(explain, '_temporary_and_external_tables._tmp_\\w+\\-\\w+\\-\\w+\\-\\w+\\-\\w+', '_temporary_and_external_tables._tmp_UNIQ_ID') FROM
(
    EXPLAIN QUERY TREE
    WITH
    a AS MATERIALIZED (SELECT * FROM users)
    SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid
);

EXPLAIN header = 1
WITH
a AS MATERIALIZED (SELECT * FROM users)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;

WITH
a AS MATERIALIZED (SELECT * FROM users)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;
