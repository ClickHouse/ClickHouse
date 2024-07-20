SET cast_keys_to_string_from_json=1;
DROP TABLE test;
CREATE TABLE test
(
    `answer` Enum8('Question' = 1, 'Answer' = 2, 'Wiki' = 3, 'TagWikiExcerpt' = 4, 'TagWiki' = 5, 'ModeratorNomination' = 6, 'WikiPlaceholder' = 7, 'PrivilegeWiki' = 8)
)
ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"answer": 1};
INSERT INTO test FORMAT JSONEachRow {"answer": "2"};
SELECT * FROM test;
