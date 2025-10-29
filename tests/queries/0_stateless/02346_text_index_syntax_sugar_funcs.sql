SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO tab VALUES (1, 'foo bar baz');
INSERT INTO tab VALUES (2, 'hey hello hi');
INSERT INTO tab VALUES (3, 'hello foo');
INSERT INTO tab VALUES (4, 'foo bar bar foo');
INSERT INTO tab VALUES (5, 'I am inverted');

-- {echoOn}
-- basic functional tests
SELECT hasToken(str, 'foo') FROM tab ORDER BY id;
SELECT str HAS_TOKEN 'foo' FROM tab ORDER BY id;

SELECT * FROM tab WHERE hasToken(str, 'foo') ORDER BY id;
SELECT * FROM tab WHERE str HAS_TOKEN 'foo' ORDER BY id;

SELECT hasAnyTokens(str, 'foo') FROM tab ORDER BY id;
SELECT str HAS_ANY_TOKENS 'foo' FROM tab ORDER BY id;

SELECT * FROM tab WHERE hasAnyTokens(str, 'foo') ORDER BY id;
SELECT * FROM tab WHERE str HAS_ANY_TOKENS 'foo' ORDER BY id;
SELECT * FROM tab WHERE str HAS_ANY_TOKENS ['foo'] ORDER BY id;

SELECT hasAllTokens(str, 'foo bar') FROM tab ORDER BY id;
SELECT str HAS_ALL_TOKENS 'foo bar' FROM tab ORDER BY id;

SELECT * FROM tab WHERE hasAllTokens(str, 'foo bar') ORDER BY id;
SELECT * FROM tab WHERE str HAS_ALL_TOKENS 'foo bar' ORDER BY id;
SELECT * FROM tab WHERE str HAS_ALL_TOKENS ['foo', 'bar'] ORDER BY id;
SELECT * FROM tab WHERE str HAS_ALL_TOKENS ['foo bar'] ORDER BY id; -- should be none

SELECT * FROM tab WHERE str HAS_ANY_TOKENS 'foo bar' ORDER BY id;
SELECT * FROM tab WHERE str HAS_ANY_TOKENS ['foo bar'] ORDER BY id; -- should be none
SELECT * FROM tab WHERE str HAS_ANY_TOKENS ['foo', 'bar'] ORDER BY id;
-- {echoOff}
