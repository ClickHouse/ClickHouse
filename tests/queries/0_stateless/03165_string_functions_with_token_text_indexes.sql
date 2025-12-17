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
SELECT '-------- GIN filter --------';
SELECT '';

SET allow_experimental_inverted_index=1;
DROP TABLE IF EXISTS 03165_token_ft;
CREATE TABLE 03165_token_ft
(
    id Int64,
    message String,
    INDEX idx_message message TYPE full_text() GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
-- Full text index works only with full parts.
SETTINGS min_bytes_for_full_part_storage=0;

INSERT INTO 03165_token_ft VALUES(1, 'Service is not ready');

SELECT '-- No skip for prefix';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv')
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE startsWith(message, 'Serv');

SELECT '';
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
SELECT '-- No skip for multiple substrings';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, ['ce', 'no'])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, ['ce', 'no']);

SELECT '';
SELECT '-- Skip for multiple substrings with complete tokens';

SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, [' wx ', ' yz '])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, [' wx ', ' yz ']);

SELECT '';
SELECT '-- No skip for multiple non-existsing substrings, only one with complete token';
SELECT trim(explain)
FROM (
    EXPLAIN indexes = 1 SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, [' wx ', 'yz'])
)
WHERE explain LIKE '%Parts:%';

SELECT * FROM 03165_token_ft WHERE multiSearchAny(message, [' wx ', 'yz']);
