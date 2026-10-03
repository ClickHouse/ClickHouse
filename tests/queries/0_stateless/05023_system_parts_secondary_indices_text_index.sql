-- Tags: no-parallel-replicas

-- `system.parts.secondary_indices_materialized` must handle a multistream skip index.
-- A `text` index is stored as several substreams (`.idx` plus `.dct.idx` / `.pst.idx`, and
-- `.pos.idx` when positions are enabled) and the read path opens every one of them, so the
-- column has to look at the whole layout instead of the base payload alone -- both without
-- claiming an index is materialized when a stream is missing, and without claiming it is
-- absent when all of them are there, including inside `skp_idx.packed`.

SET mutations_sync = 2;
SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_text_index;

CREATE TABLE t_text_index
(
    k UInt64,
    s String,
    s2 String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0,
         packed_skip_index_max_bytes = 0;

INSERT INTO t_text_index SELECT number, concat('hello', number % 10, ' world'), concat('foo', number % 10) FROM numbers(1000);

SELECT 'after insert', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index' AND active
ORDER BY name;

-- A second text index added afterwards is not materialized in the existing part until
-- `MATERIALIZE INDEX` writes all of its substreams.
ALTER TABLE t_text_index ADD INDEX idx_s2 s2 TYPE text(tokenizer = ngrams(3)) GRANULARITY 1;

SELECT 'after add index', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index' AND active
ORDER BY name;

ALTER TABLE t_text_index MATERIALIZE INDEX idx_s2;

SELECT 'after materialize index', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index' AND active
ORDER BY name;

DROP TABLE t_text_index;

-- The substreams of a small text index live inside the part's `skp_idx.packed` archive and
-- have no `checksums.txt` entry of their own. The index is materialized all the same.
DROP TABLE IF EXISTS t_text_index_packed;

CREATE TABLE t_text_index_packed
(
    k UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0,
         packed_skip_index_max_bytes = 1048576;

INSERT INTO t_text_index_packed SELECT number, concat('hello', number % 10, ' world') FROM numbers(1000);

SELECT 'packed', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index_packed' AND active
ORDER BY name;

DROP TABLE t_text_index_packed;

-- The column follows the reader's decision, not just the presence of a file: an index whose
-- granules were serialized under a column type the metadata no longer declares is refused by
-- `IMergeTreeIndex::getDeserializedFormat`, so it is reported as not materialized until the
-- mutation scheduled by `ALTER TABLE ... MODIFY COLUMN` has rewritten the part.
DROP TABLE IF EXISTS t_stale_index_type;

CREATE TABLE t_stale_index_type
(
    k UInt64,
    v UInt32,
    INDEX mm_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_stale_index_type SELECT number, number FROM numbers(1000);

SELECT 'before modify column', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_stale_index_type' AND active
ORDER BY name;

-- Hold the mutation back so the part still carries the granules written with the old type.
SYSTEM STOP MERGES t_stale_index_type;
ALTER TABLE t_stale_index_type MODIFY COLUMN v String SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'after modify column', secondary_indices_materialized
FROM system.parts
WHERE database = currentDatabase() AND table = 't_stale_index_type' AND active
ORDER BY name;

SYSTEM START MERGES t_stale_index_type;
DROP TABLE t_stale_index_type;
