-- Tags: no-parallel-replicas

-- `bm25()` follows the boolean structure of the filter (Lucene / Elasticsearch `bool` semantics):
-- every matching scoring predicate adds its score, a conjunction adds the scores of its predicates
-- only when all of them hold, and predicates under NOT, non-text predicates and opaque functions add nothing.

SET enable_analyzer = 1;
SET allow_experimental_bm25_scoring = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_bm25_bool;
DROP TABLE IF EXISTS tab_bm25_bool_multipart;

CREATE TABLE tab_bm25_bool
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_bm25_bool VALUES
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

-- Per-row BM25 of one predicate's tokens (k1 = 1.2, b = 0.75, Lucene-smoothed IDF), 0 for rows without any of them.
CREATE VIEW bm25_clause AS
WITH
    1.2 AS k1,
    0.75 AS b,
    (SELECT count() FROM tab_bm25_bool) AS n,
    (SELECT avg(length(tokens(body, 'splitByNonAlpha'))) FROM tab_bm25_bool) AS avgdl
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
    FROM tab_bm25_bool
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
        FROM tab_bm25_bool
        ARRAY JOIN {needles:Array(String)} AS tok
        GROUP BY tok
    )
) AS idfs ON rows.tok = idfs.tok
GROUP BY rows.id;

-- Token presence per row, to build the expected structured scores.
CREATE VIEW presence AS
SELECT
    id,
    price,
    tokens(body, 'splitByNonAlpha') AS toks,
    has(toks, 'raft') AS has_raft,
    has(toks, 'consensus') AS has_consensus,
    has(toks, 'log') AS has_log,
    has(toks, 'stream') AS has_stream,
    has(toks, 'quorum') AS has_quorum
FROM tab_bm25_bool;

SELECT '-- hasAllTokens(consensus, raft) OR hasToken(stream): the failed conjunction of row 4 adds nothing';
SELECT id, round(bm25(), 4)
FROM tab_bm25_bool
WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
ORDER BY id;

SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE hasAllTokens(body, ['consensus', 'raft']) OR hasToken(body, 'stream')
) AS direct
INNER JOIN
(
    SELECT p.id AS id, if(p.has_consensus AND p.has_raft, cr.score, 0) + if(p.has_stream, s.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['consensus', 'raft']) AS cr ON p.id = cr.id
    LEFT JOIN bm25_clause(needles = ['stream']) AS s ON p.id = s.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- (raft AND consensus) OR (raft AND log): a predicate in two clauses counts twice';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE (hasToken(body, 'raft') AND hasToken(body, 'consensus')) OR (hasToken(body, 'raft') AND hasToken(body, 'log'))
    SETTINGS optimize_extract_common_expressions = 0
) AS direct
INNER JOIN
(
    SELECT p.id AS id, if(p.has_raft AND p.has_consensus, r.score + c.score, 0) + if(p.has_raft AND p.has_log, r.score + l.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['raft']) AS r ON p.id = r.id
    LEFT JOIN bm25_clause(needles = ['consensus']) AS c ON p.id = c.id
    LEFT JOIN bm25_clause(needles = ['log']) AS l ON p.id = l.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- the score follows the filter as the analyzer rewrote it: the default factoring to raft AND (consensus OR log) counts raft once';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE (hasToken(body, 'raft') AND hasToken(body, 'consensus')) OR (hasToken(body, 'raft') AND hasToken(body, 'log'))
    SETTINGS optimize_extract_common_expressions = 1
) AS direct
INNER JOIN
(
    SELECT p.id AS id, r.score + if(p.has_consensus, c.score, 0) + if(p.has_log, l.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['raft']) AS r ON p.id = r.id
    LEFT JOIN bm25_clause(needles = ['consensus']) AS c ON p.id = c.id
    LEFT JOIN bm25_clause(needles = ['log']) AS l ON p.id = l.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- raft OR NOT consensus: the negated predicate adds nothing';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE hasToken(body, 'raft') OR NOT hasToken(body, 'consensus')
) AS direct
INNER JOIN
(
    SELECT p.id AS id, if(p.has_raft, r.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['raft']) AS r ON p.id = r.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- (raft AND price > 10) OR stream: a conjunction with a non-text predicate is masked by the whole conjunction';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE (hasToken(body, 'raft') AND price > 10) OR hasToken(body, 'stream')
) AS direct
INNER JOIN
(
    SELECT p.id AS id, if(p.has_raft AND p.price > 10, r.score, 0) + if(p.has_stream, s.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['raft']) AS r ON p.id = r.id
    LEFT JOIN bm25_clause(needles = ['stream']) AS s ON p.id = s.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- (quorum = 1) OR stream: a predicate inside an opaque function adds nothing';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE (hasToken(body, 'quorum') = 1) OR hasToken(body, 'stream')
) AS direct
INNER JOIN
(
    SELECT p.id AS id, if(p.has_stream, s.score, 0) AS score
    FROM presence AS p
    LEFT JOIN bm25_clause(needles = ['stream']) AS s ON p.id = s.id
) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- hasAnyTokens(consensus, raft) AND price > 4: the root conjunction needs no mask, the filter drops the rest';
SELECT
    direct.id,
    if(abs(direct.score - ref.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', direct.score, ref.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE hasAnyTokens(body, ['consensus', 'raft']) AND price > 4
) AS direct
INNER JOIN bm25_clause(needles = ['consensus', 'raft']) AS ref ON direct.id = ref.id
ORDER BY direct.id;

SELECT '-- the root conjunction is not masked, a nested one is';
-- The mask is `if(<conjunction>, <score>, 0)`; the name of the conjunction node depends on whether it is in PREWHERE.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, bm25() FROM tab_bm25_bool WHERE hasAnyTokens(body, ['consensus', 'raft']) AND price > 4
)
WHERE explain LIKE '%if(%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT id, bm25() FROM tab_bm25_bool WHERE (hasToken(body, 'raft') AND price > 10) OR hasToken(body, 'stream')
)
WHERE explain LIKE '%if(%';

SELECT '-- the read step reports the scoring index, the number of scoring predicates and the parameters';
SELECT count() > 0 FROM
(
    EXPLAIN SELECT id, bm25(1.5, 0.5) FROM tab_bm25_bool WHERE (hasToken(body, 'raft') AND price > 10) OR hasToken(body, 'stream')
)
WHERE explain LIKE '%BM25 scoring: index idx_body, scoring predicates: 2, k1: 1.5, b: 0.5%';

SELECT '-- the score is available in the SELECT list, in expressions and as the sort key at once';
SELECT id, round(bm25(), 4) AS score, bm25() > 1 AS is_high
FROM tab_bm25_bool
WHERE (hasToken(body, 'raft') AND price > 10) OR hasToken(body, 'stream')
ORDER BY bm25() DESC, id;

SELECT '-- part-distribution invariance for the structured score';
CREATE TABLE tab_bm25_bool_multipart
(
    id UInt32,
    body String,
    price UInt32,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

SYSTEM STOP MERGES tab_bm25_bool_multipart;

INSERT INTO tab_bm25_bool_multipart SELECT id, body, price FROM tab_bm25_bool WHERE id <= 3;
INSERT INTO tab_bm25_bool_multipart SELECT id, body, price FROM tab_bm25_bool WHERE id BETWEEN 4 AND 6;
INSERT INTO tab_bm25_bool_multipart SELECT id, body, price FROM tab_bm25_bool WHERE id >= 7;

SELECT one_part.id, if(abs(one_part.score - multi_part.score) <= 1e-4, 'OK', format('MISMATCH {} vs {}', one_part.score, multi_part.score))
FROM
(
    SELECT id, bm25() AS score FROM tab_bm25_bool
    WHERE (hasAllTokens(body, ['consensus', 'raft']) AND price > 4) OR hasToken(body, 'stream') OR NOT hasToken(body, 'log')
) AS one_part
INNER JOIN
(
    SELECT id, bm25() AS score FROM tab_bm25_bool_multipart
    WHERE (hasAllTokens(body, ['consensus', 'raft']) AND price > 4) OR hasToken(body, 'stream') OR NOT hasToken(body, 'log')
) AS multi_part ON one_part.id = multi_part.id
ORDER BY one_part.id;

DROP VIEW presence;
DROP VIEW bm25_clause;
DROP TABLE tab_bm25_bool;
DROP TABLE tab_bm25_bool_multipart;
