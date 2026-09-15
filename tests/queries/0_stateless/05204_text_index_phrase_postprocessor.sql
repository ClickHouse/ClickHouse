-- `hasPhrase` on a column with a `text` index that has a postprocessor is rewritten to run under the
-- index's analyzer. The rewrite used to postprocess the tokens of both sides and rejoin them with a
-- space for `hasPhrase` to re-tokenize, which assumed that a space is a separator of the index
-- tokenizer. For a tokenizer like `splitByString(['()'])` the rejoined haystack came back as a single
-- token and no phrase could ever match; and when a postprocessed token is itself a separator, the
-- re-split dissolved it and made tokens adjacent that the index keeps apart. The token sequences are
-- compared directly now, so every execution path agrees with the data.

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_05204;
CREATE TABLE t_05204
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByString(['()']), postprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05204 VALUES (1, 'a()bc()d'), (2, 'A()BC()D'), (3, 'a()d()bc');

SELECT 'a phrase that is present', count() FROM t_05204 WHERE hasPhrase(doc, 'bc()d');
SELECT 'without the skip index', count() FROM t_05204 WHERE hasPhrase(doc, 'bc()d') SETTINGS use_skip_indexes = 0;
SELECT 'without the direct read', count() FROM t_05204 WHERE hasPhrase(doc, 'bc()d') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT 'the postprocessor applies to the phrase too', count() FROM t_05204 WHERE hasPhrase(doc, 'BC()D');
SELECT 'a phrase whose tokens are not adjacent', count() FROM t_05204 WHERE hasPhrase(doc, 'd()bc');
SELECT 'a phrase that is absent', count() FROM t_05204 WHERE hasPhrase(doc, 'bc()x');
SELECT 'a single token as a phrase', count() FROM t_05204 WHERE hasPhrase(doc, 'bc');
-- `hasToken` keeps its own (literal, unanalyzed) meaning on the two-argument spelling, so the
-- uppercase row is not matched; that is the accepted index-versus-scan difference, not this bug.
SELECT 'hasToken is unchanged', count() FROM t_05204 WHERE hasToken(doc, 'bc');
SELECT 'hasAllTokens still works', count() FROM t_05204 WHERE hasAllTokens(doc, ['bc', 'd']);

-- A postprocessed token that is itself a separator of the tokenizer must not make its neighbours adjacent.
DROP TABLE IF EXISTS t_05204_separator;
CREATE TABLE t_05204_separator
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByString([' ', 'x']), postprocessor = lower(doc)) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05204_separator VALUES (1, 'A X B'), (2, 'A B');

SELECT 'separated by a token', count() FROM t_05204_separator WHERE hasPhrase(doc, 'a b');
SELECT 'the same without the skip index', count() FROM t_05204_separator WHERE hasPhrase(doc, 'a b') SETTINGS use_skip_indexes = 0;

-- The same tokenizer without a postprocessor is the control: the function tokenizes both sides itself.
DROP TABLE IF EXISTS t_05204_control;
CREATE TABLE t_05204_control
(
    id UInt64,
    doc String,
    INDEX idx doc TYPE text(tokenizer = splitByString(['()'])) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05204_control VALUES (1, 'a()bc()d');

SELECT 'the control', count() FROM t_05204_control WHERE hasPhrase(doc, 'bc()d');

-- And the same data with no index at all, which is what the answers above have to agree with.
DROP TABLE IF EXISTS t_05204_no_index;
CREATE TABLE t_05204_no_index (id UInt64, doc String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05204_no_index VALUES (1, 'a()bc()d'), (2, 'A()BC()D'), (3, 'a()d()bc');

SELECT 'no index at all', count() FROM t_05204_no_index WHERE hasPhrase(doc, 'bc()d');

DROP TABLE t_05204_no_index;
DROP TABLE t_05204_control;
DROP TABLE t_05204_separator;
DROP TABLE t_05204;
