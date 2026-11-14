-- Tags: no-shared-merge-tree
-- no-shared-merge-tree: uses DETACH PARTITION / ATTACH PARTITION.

-- A part written with a text index without scoring (no weights, no `.dl` document lengths) whose index
-- definition is later replaced by a scoring one: a merge cannot take such an index copy as is
-- and must rebuild the index from the source rows.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab_scoring_rebuild;

CREATE TABLE tab_scoring_rebuild
(
    id UInt32,
    body String,
    INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4, allow_experimental_text_index_scoring = 1;

SYSTEM STOP MERGES tab_scoring_rebuild;

INSERT INTO tab_scoring_rebuild VALUES
    (1, 'raft consensus raft log'),
    (2, 'consensus protocol basics'),
    (3, 'paxos consensus paxos consensus paxos overview'),
    (4, 'log replication stream raft');

-- Keep the old part's index files while the index definition changes to a scoring one.
ALTER TABLE tab_scoring_rebuild DETACH PARTITION tuple();
ALTER TABLE tab_scoring_rebuild DROP INDEX idx_body;
ALTER TABLE tab_scoring_rebuild ADD INDEX idx_body(body) TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', scoring = 'bm25');
ALTER TABLE tab_scoring_rebuild ATTACH PARTITION tuple();

INSERT INTO tab_scoring_rebuild VALUES
    (5, 'raft leader election term log'),
    (6, 'distributed consensus raft raft raft quorum');

SELECT '-- plain filtering on the old part still works';
SELECT count() FROM tab_scoring_rebuild WHERE hasToken(body, 'raft');

SELECT '-- before the merge, the old part has no weights';
SELECT groupArray(row_id) FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_rebuild, idx_body) WHERE token = 'raft' GROUP BY part_name ORDER BY part_name;
-- An aggregate, so that no rows of the new part reach the client before the exception from the old part.
SELECT count() FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_rebuild, idx_body) WHERE weight > 1; -- { serverError BAD_ARGUMENTS }

SYSTEM START MERGES tab_scoring_rebuild;
OPTIMIZE TABLE tab_scoring_rebuild FINAL;

SELECT '-- the merged part has the index with the weights';
SELECT token, row_id, weight FROM mergeTreeTextIndex(currentDatabase(), tab_scoring_rebuild, idx_body) WHERE token IN ('raft', 'consensus') ORDER BY token DESC, row_id;

DROP TABLE tab_scoring_rebuild;
