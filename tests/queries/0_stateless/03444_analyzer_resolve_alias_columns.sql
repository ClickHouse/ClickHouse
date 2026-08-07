CREATE TABLE users (
    uid Int16,
    name String,
    age Int16,
    v Array(Int16) ALIAS arrayMap(x -> age, array(name))
) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT * FROM users FORMAT Null;

CREATE TABLE out1
  (
    id UInt64,
    j JSON,
    name Array(UInt32) ALIAS arrayMap(x -> toUInt32(x), JSONAllPaths(j)),
    value Array(Array(UInt32)) ALIAS arrayMap(x -> JSONExtract(CAST(j, 'String'), indexOf(name, x), 'Array(UInt32)'), name)
)
ORDER BY id;

INSERT INTO out1 SELECT 42, '{"a" : 42}';

SELECT * FROM out1 FORMAT Null;
