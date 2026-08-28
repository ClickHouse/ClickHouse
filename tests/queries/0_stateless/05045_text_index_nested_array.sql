-- Tests that a text index cannot be created on a multidimensional array with a String/FixedString
-- base type. The runtime cannot correctly tokenize these columns, so they are rejected at validation
-- for new CREATE and ALTER ADD INDEX.
-- (Legacy tables that already carry such an index still attach; see 05046_text_index_nested_array_attach.)

DROP TABLE IF EXISTS tab;

-- Rejected: multidimensional arrays with a String/FixedString base type

CREATE TABLE tab (t Array(Array(String)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (t Array(Array(FixedString(8))), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (t Array(Array(Nullable(String))), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (t Array(Array(Array(String))), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Rejected via ALTER ADD INDEX as well

CREATE TABLE tab (t Array(Array(String))) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab;

CREATE TABLE tab (t Array(Array(FixedString(8)))) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE tab ADD INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
DROP TABLE tab;

-- Accepted: scalars and single-level arrays

CREATE TABLE tab (t String, INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (t FixedString(8), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (t Array(String), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (t Array(FixedString(8)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (t Array(Nullable(String)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

CREATE TABLE tab (t Array(LowCardinality(String)), INDEX idx t TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tab;

SELECT 'ok';
