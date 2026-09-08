-- Tags: no-parallel-replicas

SET enable_analyzer = 1;
SET allow_experimental_bm25_score_column = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes_on_data_read = 1;

DROP TABLE IF EXISTS tab_scoring_rebuild;
DROP TABLE IF EXISTS tab_scoring_ref;

-- A part written with a non-scoring text index (no `.dl` doc lengths) whose index definition is later
-- replaced by a scoring one: a merge cannot take such an index copy as is (the document lengths of its
-- rows are unknown) and must rebuild the index from the source rows.
CREATE TABLE tab_scoring_rebuild
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_scoring_rebuild VALUES
    (1, 'raft consensus raft log'),
    (2, 'consensus protocol basics'),
    (3, 'paxos consensus paxos consensus paxos overview'),
    (4, 'log replication stream raft');

-- Keep the old part's index files while the index definition changes to a scoring one.
ALTER TABLE tab_scoring_rebuild DETACH PARTITION tuple();
ALTER TABLE tab_scoring_rebuild DROP INDEX idx_body;
ALTER TABLE tab_scoring_rebuild ADD INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1;
ALTER TABLE tab_scoring_rebuild ATTACH PARTITION tuple();

INSERT INTO tab_scoring_rebuild VALUES
    (5, 'raft leader election term log'),
    (6, 'distributed consensus raft raft raft quorum');

SELECT '-- plain filtering on the old part still works';
SELECT count() FROM tab_scoring_rebuild WHERE hasToken(body, 'raft');

SELECT '-- before the merge, the old part has no scoring data';
SELECT id, _bm25_score FROM tab_scoring_rebuild WHERE hasToken(body, 'raft'); -- { serverError BAD_ARGUMENTS }

OPTIMIZE TABLE tab_scoring_rebuild FINAL;

-- The reference: the same rows indexed with scoring from the start.
CREATE TABLE tab_scoring_ref
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', enable_scoring = 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

INSERT INTO tab_scoring_ref SELECT id, body FROM tab_scoring_rebuild;

SELECT '-- scores over the merged part match a scoring index built from scratch';
SELECT
    merged.id,
    if(abs(merged.score - ref.score) <= 1e-6, 'OK', format('MISMATCH {} vs {}', merged.score, ref.score))
FROM
(
    SELECT id, _bm25_score AS score FROM tab_scoring_rebuild
    WHERE hasAnyTokens(body, ['consensus', 'raft'])
) AS merged
INNER JOIN
(
    SELECT id, _bm25_score AS score FROM tab_scoring_ref
    WHERE hasAnyTokens(body, ['consensus', 'raft'])
) AS ref ON merged.id = ref.id
ORDER BY merged.id;

DROP TABLE tab_scoring_rebuild;
DROP TABLE tab_scoring_ref;
