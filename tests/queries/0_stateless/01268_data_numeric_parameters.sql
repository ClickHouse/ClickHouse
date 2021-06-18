DROP TABLE IF EXISTS ints;
DROP TABLE IF EXISTS floats;
DROP TABLE IF EXISTS strings;

CREATE TABLE ints (
    a TINYINT,
    b TINYINT(8),
    c SMALLINT,
    d SMALLINT(16),
    e INT,
    f INT(32),
    g BIGINT,
    h BIGINT(64)
) engine=Memory;

INSERT INTO ints VALUES (1, 8, 11, 16, 21, 32, 41, 64);

SELECT  toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f), toTypeName(g), toTypeName(h) FROM ints;

CREATE TABLE floats (
    a FLOAT,
    b FLOAT(12),
    c FLOAT(15, 22),
    d DOUBLE,
    e DOUBLE(12),
    f DOUBLE(4, 18)

) engine=Memory;

INSERT INTO floats VALUES (1.1, 1.2, 1.3, 41.1, 41.1, 42.1);

SELECT  toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f) FROM floats;


CREATE TABLE strings (
    a VARCHAR,
    b VARCHAR(11)
) engine=Memory;

INSERT INTO strings VALUES ('test', 'string');

SELECT  toTypeName(a), toTypeName(b)  FROM strings;
