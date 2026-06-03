-- Tags: no-replicated-database, no-parallel

DROP USER IF EXISTS test_03727;
CREATE USER test_03727;

CREATE TABLE normal
(
    n Int32,
    s String
)
ENGINE = MergeTree()
ORDER BY n;

CREATE TABLE secret
(
    s String
)
ENGINE = MergeTree()
ORDER BY s;

INSERT INTO normal VALUES (1, '');
INSERT INTO secret VALUES ('secret');

GRANT ALTER UPDATE ON normal TO test_03727;
GRANT READ ON REMOTE to test_03727;
GRANT CREATE TEMPORARY TABLE ON *.* TO test_03727;

EXECUTE AS test_03727 ALTER TABLE normal UPDATE s = (SELECT * FROM remote('localhost', currentDatabase(), 'secret') LIMIT 1) WHERE n=1; -- { serverError ACCESS_DENIED }

DROP USER IF EXISTS test_03727;
