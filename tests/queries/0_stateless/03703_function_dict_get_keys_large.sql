DROP DICTIONARY IF EXISTS dict_big;
DROP TABLE IF EXISTS dict_src_big;

CREATE TABLE dict_src_big
(
    id        UInt64,
    grp       String,
    grp_round String
) ENGINE = Memory;

INSERT INTO dict_src_big
SELECT
    number AS id,
    toString(number) AS grp,
    toString(number % 2000) AS grp_round
FROM numbers(100000);

CREATE DICTIONARY dict_big
(
    id        UInt64,
    grp       String,
    grp_round String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_src_big'))
LIFETIME(0)
LAYOUT(HASHED());

SELECT 'Single exact match';
SELECT length(dictGetKeys('dict_big', 'grp', '123')) ;
SELECT arraySum(dictGetKeys('dict_big', 'grp', '123'));

SELECT 'Missing value';
SELECT length(dictGetKeys('dict_big', 'grp', '100000'));
SELECT arraySum(dictGetKeys('dict_big', 'grp', '100000'));

SELECT 'Multiple matches for same attribute value';
SELECT length(dictGetKeys('dict_big', 'grp_round', '7'));
SELECT arraySum(dictGetKeys('dict_big', 'grp_round', '7'));

SELECT 'Vector of inputs with mixed hits and misses';
SELECT sum(length(dictGetKeys('dict_big', 'grp', toString(number))))
FROM numbers(5);

SELECT sum(arraySum(dictGetKeys('dict_big', 'grp', toString(number))))
FROM numbers(3);

SELECT 'All sum';
SELECT sum(arraySum(dictGetKeys('dict_big', 'grp', toString(number))))
FROM numbers(100000);

SELECT 'Misc';
SELECT sum(length(dictGetKeys('dict_big', 'grp_round', '7')))
FROM numbers(2);

SELECT length(dictGetKeys('dict_big', 'grp_round', '1999'));
SELECT arraySum(dictGetKeys('dict_big', 'grp_round', '1999'));
