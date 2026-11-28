-- Tags: no-parallel-replicas

-- `bm25()` when a scoring predicate cannot match in some part: the document frequencies of its tokens
-- still count every part, and the scores of the other predicates do not depend on the unmatched one.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_unmatched;

-- One token per dictionary block, so the tokens of a predicate are looked up in separate blocks.
CREATE TABLE tab_bm25_unmatched
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25', dictionary_block_size = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

SYSTEM STOP MERGES tab_bm25_unmatched;

-- A part with `zeta` and without `beta`.
INSERT INTO tab_bm25_unmatched VALUES (1, 'zeta one'), (2, 'zeta zeta two'), (3, 'three'), (4, 'gamma');
-- A part with both.
INSERT INTO tab_bm25_unmatched VALUES (5, 'beta zeta'), (6, 'beta five'), (7, 'zeta six'), (8, 'gamma beta zeta');
-- A part where `aa` and `bb` are never in the same row, with enough rows to store their postings out of the dictionary.
INSERT INTO tab_bm25_unmatched SELECT 100 + number, if(number % 2 = 0, 'aa cc', 'bb cc') FROM numbers(16);
INSERT INTO tab_bm25_unmatched SELECT 200 + number, 'dd cc' FROM numbers(4);
-- A part where `zz` is only in the rows filtered out by the primary key.
INSERT INTO tab_bm25_unmatched SELECT 300 + number, if(number < 8, 'yy zz', 'yy') FROM numbers(40);

-- Per-row BM25 of one predicate's tokens over the whole table (k1 = 1.2, b = 0.75, Lucene-smoothed IDF).
CREATE VIEW bm25_unmatched_clause AS
WITH
    1.2 AS k1,
    0.75 AS b,
    (SELECT count() FROM tab_bm25_unmatched) AS n,
    (SELECT avg(length(tokens(body, 'splitByNonAlpha'))) FROM tab_bm25_unmatched) AS avgdl
SELECT
    rows.id AS id,
    sum(idfs.idf * (k1 + 1) * rows.tf / (rows.tf + k1 * (1 - b + b * rows.dl / avgdl))) AS score
FROM
(
    SELECT
        id,
        tok,
        countEqual(tokens(body, 'splitByNonAlpha'), tok) AS tf,
        length(tokens(body, 'splitByNonAlpha')) AS dl
    FROM tab_bm25_unmatched
    ARRAY JOIN {needles:Array(String)} AS tok
) AS rows
INNER JOIN
(
    SELECT
        tok,
        ln((n - df + 0.5) / (df + 0.5) + 1) AS idf
    FROM
    (
        SELECT tok, countIf(has(tokens(body, 'splitByNonAlpha'), tok)) AS df
        FROM tab_bm25_unmatched
        ARRAY JOIN {needles:Array(String)} AS tok
        GROUP BY tok
    )
) AS idfs ON rows.tok = idfs.tok
GROUP BY rows.id;

SELECT '-- beta AND zeta: the part without beta still counts in the document frequency of zeta';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_unmatched
    WHERE hasToken(body, 'beta') AND hasToken(body, 'zeta')
) AS direct
INNER JOIN bm25_unmatched_clause(needles = ['beta', 'zeta']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasAllTokens(beta, zeta) OR gamma: the part without beta still counts in the document frequency of zeta';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_unmatched
    WHERE hasAllTokens(body, ['beta', 'zeta']) OR hasToken(body, 'gamma')
) AS direct
INNER JOIN
(
    SELECT bz.id AS id, if(has(tokens(t.body, 'splitByNonAlpha'), 'beta') AND has(tokens(t.body, 'splitByNonAlpha'), 'zeta'), bz.score, 0) + g.score AS score
    FROM tab_bm25_unmatched AS t
    INNER JOIN bm25_unmatched_clause(needles = ['beta', 'zeta']) AS bz ON t.id = bz.id
    INNER JOIN bm25_unmatched_clause(needles = ['gamma']) AS g ON t.id = g.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasAllTokens(aa, bb, cc) OR dd: the conjunction with disjoint postings adds nothing';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_unmatched
    WHERE hasAllTokens(body, ['aa', 'bb', 'cc']) OR hasToken(body, 'dd')
) AS direct
INNER JOIN bm25_unmatched_clause(needles = ['dd']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasAnyTokens(yy, zz) AND id > 320: zz is absent from the rows left by the primary key';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_unmatched
    WHERE hasAnyTokens(body, ['yy', 'zz']) AND id > 320
) AS direct
INNER JOIN bm25_unmatched_clause(needles = ['yy', 'zz']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

DROP VIEW bm25_unmatched_clause;
DROP TABLE tab_bm25_unmatched;
