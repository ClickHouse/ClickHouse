DROP TABLE IF EXISTS test_type_alias;

CREATE TABLE test_type_alias(
  binary BINARY(1),
  dec DEC(1, 1),
  tinyint TINYINT,
  smallint SMALLINT,
  int INT,
  integer INTEGER,
  bigint BIGINT,
  float FLOAT,
  double DOUBLE,
  char CHAR,
  varchar VARCHAR,
  text TEXT,
  tinytext TINYTEXT,
  mediumtext MEDIUMTEXT,
  longtext LONGTEXT,
  blob BLOB,
  tinyblob TINYBLOB,
  mediumblob MEDIUMBLOB,
  longblob LONGBLOB
) ENGINE = Memory;

DESC test_type_alias;

DETACH TABLE test_type_alias;
ATTACH TABLE test_type_alias;

DESC test_type_alias;

DROP TABLE IF EXISTS test_type_alias;
