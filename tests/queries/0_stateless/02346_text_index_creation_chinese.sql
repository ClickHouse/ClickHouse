-- Tags: no-fasttest
-- no-fasttest: the chinese tokenizer uses Jieba (cppjieba), which is not available in the fast test build.

-- Companion to 02346_text_index_creation for the `chinese` tokenizer, which can only be
-- exercised in jieba-enabled builds. Mirrors the asciiCJK creation tests: identifier and
-- function forms, with and without the granularity argument, plus invalid arguments.

DROP TABLE IF EXISTS tab;

SELECT 'Test chinese tokenizer.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = 'chinese')
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese)
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese())
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT 'Test chinese tokenizer granularity argument.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese('coarse_grained'))
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese('fine_grained'))
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese(fine_grained))
)
ENGINE = MergeTree
ORDER BY tuple();
DROP TABLE tab;

SELECT '-- invalid granularity is rejected.';

CREATE TABLE tab
(
    str String,
    INDEX idx str TYPE text(tokenizer = chinese('invalid')) -- { serverError BAD_ARGUMENTS }
)
ENGINE = MergeTree
ORDER BY tuple();
