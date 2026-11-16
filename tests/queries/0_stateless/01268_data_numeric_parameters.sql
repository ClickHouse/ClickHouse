DROP TABLE IF EXISTS ints;
DROP TABLE IF EXISTS floats;
DROP TABLE IF EXISTS strings;

-- Every integer alias that accepts the MySQL display-width modifier (N), both bare
-- and with (N). The (N) must be accepted and ignored, never alter the resulting type.
CREATE TABLE ints (
    a TINYINT,
    b TINYINT(8),
    c SMALLINT,
    d SMALLINT(16),
    e MEDIUMINT,
    f MEDIUMINT(24),
    g INT,
    h INT(32),
    i INT(),
    j INTEGER,
    k INTEGER(11),
    l BIGINT,
    m BIGINT(64),
    n INT1,
    o INT1(8)
) engine=Memory;

INSERT INTO ints VALUES (1, 8, 11, 16, 21, 24, 31, 32, 33, 41, 11, 51, 64, 71, 8);

SELECT  toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f), toTypeName(g), toTypeName(h), toTypeName(i), toTypeName(j), toTypeName(k), toTypeName(l), toTypeName(m), toTypeName(n), toTypeName(o) FROM ints;

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

DROP TABLE floats;
DROP TABLE ints;
DROP TABLE strings;
