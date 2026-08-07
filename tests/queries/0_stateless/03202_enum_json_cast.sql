DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    `answer` Enum8('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8)
)
ENGINE = Memory;

INSERT INTO test FORMAT JSONEachRow {"answer": 1};

INSERT INTO test FORMAT JSONEachRow {"answer": "2"};

SELECT * FROM test ORDER BY ALL;

DROP TABLE test;
CREATE TABLE test
(
    `answer` Enum8('1' = 2, '2' = 1, 'Wiki' = 3)
)
ENGINE = Memory;

INSERT INTO test FORMAT JSONEachRow {"answer": 1};

INSERT INTO test FORMAT JSONEachRow {"answer": "2"};

SELECT * FROM test ORDER BY ALL;
