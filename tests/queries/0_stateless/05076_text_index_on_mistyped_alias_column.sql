-- A text index over an ALIAS column whose declared type differs from the type its expression
-- produces can never be used: reading the column applies an implicit CAST to the declared type,
-- while the index is built over the expression as written, and the two no longer match by name.
-- The index was silently built, merged and never read. Reject it instead.
-- https://github.com/ClickHouse/ClickHouse/issues/111643

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_mistyped_alias;
DROP TABLE IF EXISTS t_matching_alias;

-- `JSONExtractKeys` returns `Array(String)`, so declaring `paths` as `String` is the mismatch.
CREATE TABLE t_mistyped_alias
(
    event String,
    paths String ALIAS JSONExtractKeys(event),
    INDEX fts_paths paths TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- Declaring the column with the type its expression produces is accepted, and the index is used.
CREATE TABLE t_matching_alias
(
    event String,
    paths Array(String) ALIAS JSONExtractKeys(event),
    INDEX fts_paths paths TYPE text(tokenizer = array)
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_matching_alias VALUES ('{"some":"value"}'), ('{"foo":"bar"}');

SELECT count() FROM t_matching_alias WHERE hasAllTokens(paths, ['xoo']);
SELECT count() FROM t_matching_alias WHERE hasAllTokens(paths, ['foo']);

-- An ALIAS column that is not indexed keeps converting as before.
DROP TABLE IF EXISTS t_alias_no_index;
CREATE TABLE t_alias_no_index
(
    event String,
    paths String ALIAS JSONExtractKeys(event)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_no_index VALUES ('{"foo":"bar"}');
SELECT paths FROM t_alias_no_index;

DROP TABLE t_matching_alias;
DROP TABLE t_alias_no_index;
