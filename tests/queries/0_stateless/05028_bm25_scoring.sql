-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25;
DROP TABLE IF EXISTS tab_bm25_multipart;

CREATE TABLE tab_bm25
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25 VALUES
    (1, 'raft consensus raft log', 5),
    (2, 'consensus protocol basics', 20),
    (3, 'paxos consensus paxos consensus paxos overview', 8),
    (4, 'log replication stream raft', 3),
    (5, 'raft leader election term log', 15),
    (6, 'kv store engine internals', 2),
    (7, 'distributed consensus raft raft raft quorum', 30),
    (8, 'stream processing pipeline notes', 7),
    (9, 'query planner and optimizer', 12),
    (10, 'vector search with quorum reads', 4);

-- Per-row BM25 reference computed in SQL over the same collection (Lucene-smoothed IDF).
-- Doc lengths in the fixture are far below the SmallFloat exact range, so no quantization error.
CREATE VIEW bm25_reference_params AS
WITH
    {k1:Float64} AS k1,
    {b:Float64} AS b,
    (SELECT count() FROM tab_bm25) AS n,
    (SELECT avg(length(tokens(body, 'splitByNonAlpha'))) FROM tab_bm25) AS avgdl
SELECT
    per_row.id AS id,
    sum(idfs.idf * (k1 + 1) * per_row.tf / (per_row.tf + k1 * (1 - b + b * per_row.dl / avgdl))) AS score
FROM
(
    SELECT
        id,
        tok,
        countEqual(tokens(body, 'splitByNonAlpha'), tok) AS tf,
        length(tokens(body, 'splitByNonAlpha')) AS dl
    FROM tab_bm25
    ARRAY JOIN {needles:Array(String)} AS tok
    WHERE tf > 0
) AS per_row
INNER JOIN
(
    SELECT
        tok,
        ln((n - df + 0.5) / (df + 0.5) + 1) AS idf
    FROM
    (
        SELECT tok, countIf(has(tokens(body, 'splitByNonAlpha'), tok)) AS df
        FROM tab_bm25
        ARRAY JOIN {needles:Array(String)} AS tok
        GROUP BY tok
    )
) AS idfs ON per_row.tok = idfs.tok
GROUP BY per_row.id;

CREATE VIEW bm25_reference AS SELECT * FROM bm25_reference_params(needles = {needles:Array(String)}, k1 = 1.2, b = 0.75);

SELECT '-- hasAnyTokens (generic scorer): scores match the SQL reference';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAnyTokens(body, ['consensus', 'raft'])
) AS direct
INNER JOIN bm25_reference(needles = ['consensus', 'raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasAllTokens (conjunction scorer): scores match the SQL reference';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAllTokens(body, ['consensus', 'raft'])
) AS direct
INNER JOIN bm25_reference(needles = ['consensus', 'raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasToken: scores match the SQL reference';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasToken(body, 'raft')
) AS direct
INNER JOIN bm25_reference(needles = ['raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- AND of two hasToken (conjunction scorer over both tokens)';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasToken(body, 'consensus') AND hasToken(body, 'raft')
) AS direct
INNER JOIN bm25_reference(needles = ['consensus', 'raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- OR composition: every matching clause adds its score, a failed conjunction adds nothing';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
) AS direct
INNER JOIN
(
    SELECT
        tab_bm25.id AS id,
        if(hasAll(tokens(body, 'splitByNonAlpha'), ['consensus', 'raft']), all_ref.score, 0) + if(has(tokens(body, 'splitByNonAlpha'), 'stream'), stream_ref.score, 0) AS score
    FROM tab_bm25
    LEFT JOIN bm25_reference(needles = ['consensus', 'raft']) AS all_ref ON tab_bm25.id = all_ref.id
    LEFT JOIN bm25_reference(needles = ['stream']) AS stream_ref ON tab_bm25.id = stream_ref.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- OR with a regular-column branch: a row surviving only via it scores 0 where it matches no scoring token';
SELECT id, round(bm25(), 4) = 0 AS is_zero
FROM tab_bm25
WHERE (hasToken(body, 'quorum') OR price < 4) AND NOT has(tokens(body, 'splitByNonAlpha'), 'quorum')
ORDER BY id;

SELECT '-- ORDER BY bm25() DESC LIMIT: plain top-k works';
SELECT id
FROM tab_bm25
WHERE hasAnyTokens(body, ['consensus', 'raft'])
ORDER BY bm25() DESC, id
LIMIT 3;

SELECT '-- bm25() used only in ORDER BY (not in SELECT)';
SELECT id
FROM tab_bm25
WHERE hasToken(body, 'consensus')
ORDER BY bm25() DESC, id
LIMIT 2;

SELECT '-- bm25() inside an expression';
SELECT id, bm25() > 0
FROM tab_bm25
WHERE hasToken(body, 'quorum')
ORDER BY id;

SELECT '-- bm25() with explicit parameters matches a reference computed with them';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25(1.5, 0.5) AS score FROM tab_bm25
    WHERE hasAnyTokens(body, ['consensus', 'raft'])
) AS direct
INNER JOIN bm25_reference_params(needles = ['consensus', 'raft'], k1 = 1.5, b = 0.5) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- the same score under two spellings of the call';
SELECT id, bm25() = bm25(1.2, 0.75) FROM tab_bm25 WHERE hasToken(body, 'quorum') ORDER BY bm25(1.2, 0.75) DESC, id;

SELECT '-- PREWHERE on a regular column composes with the score';
SELECT id, bm25() > 0
FROM tab_bm25
PREWHERE price >= 5
WHERE hasAnyTokens(body, ['consensus', 'raft'])
ORDER BY bm25() DESC, id;

SELECT '-- part-distribution invariance: identical scores for 1-part and multi-part layouts';
-- The middle part misses the token `consensus`, so the All-mode query fails there and detaches its
-- tokens; row 4 still survives via `stream` and must receive the contribution of its token `raft`.
CREATE TABLE tab_bm25_multipart
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

SYSTEM STOP MERGES tab_bm25_multipart;

INSERT INTO tab_bm25_multipart SELECT id, body, price FROM tab_bm25 WHERE id <= 3;
INSERT INTO tab_bm25_multipart SELECT id, body, price FROM tab_bm25 WHERE id BETWEEN 4 AND 6;
INSERT INTO tab_bm25_multipart SELECT id, body, price FROM tab_bm25 WHERE id >= 7;

SELECT one_part.id, if(abs(one_part.score - multi_part.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', one_part.score, multi_part.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
) AS one_part
INNER JOIN
(
    SELECT id, bm25() AS score FROM tab_bm25_multipart
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
) AS multi_part ON one_part.id = multi_part.id
ORDER BY one_part.id;

SELECT '-- same result when skip indexes are applied during the planning-time analysis';
SELECT one_part.id, if(abs(one_part.score - multi_part.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', one_part.score, multi_part.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
    SETTINGS use_skip_indexes_on_data_read = 0
) AS one_part
INNER JOIN
(
    SELECT id, bm25() AS score FROM tab_bm25_multipart
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
    SETTINGS use_skip_indexes_on_data_read = 0
) AS multi_part ON one_part.id = multi_part.id
ORDER BY one_part.id;

SELECT '-- lightweight delete: scoring is rejected while the part carries a deleted-rows mask';
DELETE FROM tab_bm25 WHERE id = 7;
SELECT id, bm25() > 0
FROM tab_bm25
WHERE hasAnyTokens(body, ['consensus', 'raft'])
ORDER BY bm25() DESC, id; -- { serverError BAD_ARGUMENTS }

SELECT '-- after the deleted rows are merged away, scoring matches the fresh statistics';
OPTIMIZE TABLE tab_bm25 FINAL;
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25
    WHERE hasAnyTokens(body, ['consensus', 'raft'])
) AS direct
INNER JOIN bm25_reference(needles = ['consensus', 'raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

DROP VIEW bm25_reference;
DROP VIEW bm25_reference_params;
DROP TABLE tab_bm25;
DROP TABLE tab_bm25_multipart;
