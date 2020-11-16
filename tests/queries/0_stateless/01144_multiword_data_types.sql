DROP TABLE IF EXISTS multiword_types;
DROP TABLE IF EXISTS unsigned_types;

CREATE TABLE multiword_types (
    a DOUBLE,
    b DOUBLE PRECISION,
    c CHAR DEFAULT 'str',
    d CHAR VARYING,
    e CHAR LARGE OBJECT COMMENT 'comment',
    f CHARACTER VARYING(123),
    g ChArAcTeR   large    OBJECT,
    h nchar varying (456) default toString(a) comment 'comment',
    i NCHAR LARGE OBJECT,
    j BINARY LARGE OBJECT,
    k BINARY VARYING,
    l NATIONAL CHAR,
    m NATIONAL CHARACTER,
    n NATIONAL CHARACTER LARGE OBJECT,
    o NATIONAL CHARACTER VARYING,
    p NATIONAL CHAR VARYING
) ENGINE=Memory;

SHOW CREATE TABLE multiword_types;

INSERT INTO multiword_types(a) VALUES (1);
SELECT toTypeName((*,)) FROM multiword_types;

CREATE TABLE unsigned_types (
    a TINYINT  SIGNED,
    b INT1     SIGNED,
    c SMALLINT SIGNED,
    d INT      SIGNED,
    e INTEGER  SIGNED,
    f BIGINT   SIGNED,
    g TINYINT  UNSIGNED,
    h INT1     UNSIGNED,
    i SMALLINT UNSIGNED,
    j INT      UNSIGNED,
    k INTEGER  UNSIGNED,
    l BIGINT   UNSIGNED
) ENGINE=Memory;

SHOW CREATE TABLE unsigned_types;

INSERT INTO unsigned_types(a) VALUES (1);
SELECT toTypeName((*,)) FROM unsigned_types;

SELECT CAST('42' AS DOUBLE PRECISION), CAST(42, 'NATIONAL CHARACTER VARYING'), CAST(-1 AS tinyint  UnSiGnEd), CAST(65535, ' sMaLlInT  signed ');

DROP TABLE multiword_types;
DROP TABLE unsigned_types;
