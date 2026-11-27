SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

WITH
a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM a WHERE uid in (SELECT uid FROM a);

WITH
a AS MATERIALIZED (SELECT uid FROM users WHERE uid IN (SELECT number FROM numbers(10)))
SELECT count() FROM a as l LEFT SEMI JOIN a AS r ON l.uid = r.uid;

WITH
    a AS MATERIALIZED (SELECT uid FROM users),
    b AS MATERIALIZED (SELECT uid FROM a)
SELECT count() FROM b as l LEFT SEMI JOIN b AS r ON l.uid = r.uid;

WITH
    b AS MATERIALIZED (SELECT uid FROM a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b as l LEFT SEMI JOIN b AS r ON l.uid = r.uid;

WITH
    b AS MATERIALIZED (SELECT uid FROM a, a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM b as l LEFT SEMI JOIN b AS r ON l.uid = r.uid;

WITH
    a AS MATERIALIZED (SELECT uid FROM users),
    b AS MATERIALIZED (SELECT uid FROM a, a)
SELECT count() FROM b as l LEFT SEMI JOIN b AS r ON l.uid = r.uid;

WITH
    b AS MATERIALIZED (SELECT uid FROM a, a),
    a AS MATERIALIZED (SELECT uid FROM users)
SELECT count() FROM (SELECT * FROM b WHERE uid in b) as l LEFT SEMI JOIN a AS r ON l.uid = r.uid;
