SELECT '-------- Bloom filter --------';
SELECT '';
DROP TABLE IF EXISTS 03165_token_bf;

SET allow_experimental_full_text_index=1;

CREATE TABLE 03165_token_bf
(
    id Int64,
    message String,
    INDEX idx_message message TYPE tokenbf_v1(32768, 3, 2) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO 03165_token_bf VALUES(1, 'Service is not ready');

SELECT '-- No skip for prefix';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE startsWith(message, 'Serv')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE startsWith(message, 'Serv');

SELECT '';
SELECT '-- Skip for prefix with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE startsWith(message, 'Serv i')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE startsWith(message, 'Serv i');

SELECT '';
SELECT '-- No skip for suffix';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE endsWith(message, 'eady')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE endsWith(message, 'eady');

SELECT '';
SELECT '-- Skip for suffix with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE endsWith(message, ' eady')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE endsWith(message, ' eady');

SELECT '';
SELECT '-- No skip for substring';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE match(message, 'no')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE match(message, 'no');

SELECT '';
SELECT '-- Skip for substring with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE match(message, ' xyz ')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE match(message, ' xyz ');

SELECT '';
SELECT '-- No skip for multiple substrings';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, ['ce', 'no'])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, ['ce', 'no']);

SELECT '';
SELECT '-- Skip for multiple substrings with complete tokens';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, [' wx ', ' yz '])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, [' wx ', ' yz ']);

SELECT '';
SELECT '-- No skip for multiple non-existsing substrings, only one with complete token';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, [' wx ', 'yz'])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_bf WHERE multiSearchAny(message, [' wx ', 'yz']);

DROP TABLE IF EXISTS 03165_token_bf;

SELECT '';
SELECT '-------- Text index filter --------';
SELECT '';

SET allow_experimental_full_text_index = 1;
DROP TABLE IF EXISTS 03165_token_ft;
CREATE TABLE 03165_token_ft
(
    id Int64,
    message String,
    INDEX idx_message message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO 03165_token_ft VALUES(1, 'Service is not ready');

-- text search cannot operate on substrings, so no filtering based on text index should be performed here
SELECT '-- No skip for prefix';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv');

SELECT '';
-- here we get one full token, so we can utilize text index to skip (in this case) all granules
SELECT '-- Skip for prefix with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv i')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv i');

SELECT '';
SELECT '-- No skip for suffix';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE endsWith(message, 'eady')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE endsWith(message, 'eady');

SELECT '';
SELECT '-- Skip for suffix with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE endsWith(message, ' eady')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE endsWith(message, ' eady');

SELECT '';
SELECT '-- No skip for substring';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE match(message, 'no')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE match(message, 'no');

SELECT '';
SELECT '-- Skip for substring with complete token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE match(message, ' xyz ')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE match(message, ' xyz ');

SELECT '';
SELECT '-- Skip for like with non matching tokens';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE like(message, '%rvice is definitely rea%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE like(message, '%rvice is definitely rea%');

SELECT '';
SELECT '-- No skip for like with matching substring';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE like(message, '%rvi%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE like(message, '%rvi%');

SELECT '';
SELECT '-- No skip for like with non-matching string';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE like(message, '%foo%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE like(message, '%foo%');

SELECT '';
SELECT '-- No skip for notLike with non-matching token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE notLike(message, '%rvice is rea%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE notLike(message, '%rvice is rea%');

SELECT '';
-- could be an optimization in the future
SELECT '-- No skip for notLike with matching tokens';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE notLike(message, '%rvice is not rea%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE notLike(message, '%rvice is not rea%');

SELECT '';
SELECT '-- No skip for notLike with matching substring';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE notLike(message, '%ready%')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE notLike(message, '%ready%');

SELECT '';
SELECT '-- No skip for equals with matching string';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE equals(message, 'Service is not ready')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE equals(message, 'Service is not ready');

SELECT '';
SELECT '-- Skip for equals with non-matching string';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE equals(message, 'Service is not rea')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE equals(message, 'Service is not rea');

SELECT '';
SELECT '-- No skip for notEquals with non-matching string';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE notEquals(message, 'Service is not rea')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE notEquals(message, 'Service is not rea');

SELECT '';
SELECT '-- No skip for notEquals with matching string';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE notEquals(message, 'Service is not ready')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE notEquals(message, 'Service is not ready');

SELECT '';
SELECT '-- No skip for hasTokenOrNull with matching token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'ready')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'ready');

SELECT '';
SELECT '-- Skip for hasTokenOrNull with non-matching token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'foo')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'foo');

SELECT '';
SELECT '-- Skip for hasTokenOrNull with ill-formed token';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'rea dy')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE hasTokenOrNull(message, 'rea dy');
