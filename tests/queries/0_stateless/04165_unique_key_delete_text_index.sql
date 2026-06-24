-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage, no-fasttest
-- A text-search read on a UNIQUE KEY table must honour the delete bitmap. The
-- text-index read path rebuilds the storage snapshot to add virtual columns; if
-- that rebuild drops the snapshot's per-partition pins, the read treats every
-- partition as fully live and returns rows a prior DELETE marked dead.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

-- Force the text-index direct-read path (createReadTasksForTextIndex) deterministically.
-- clickhouse-test randomizes both of these; with either off the hasToken query can take
-- a different read path and the test would stop discriminating the snapshot-pin-drop bug.
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS uk_txt;

CREATE TABLE uk_txt
(
    id UInt64,
    txt String,
    INDEX txt_idx txt TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_txt;

INSERT INTO uk_txt VALUES (1, 'alpha beta'), (2, 'alpha gamma'), (3, 'alpha epsilon');

DELETE FROM uk_txt WHERE id = 2;

-- hasToken routes through the text-index read path; the deleted row (id 2) must
-- not surface even though it still carries the 'alpha' token in the index.
SELECT 'token_search' AS step, id FROM uk_txt WHERE hasToken(txt, 'alpha') ORDER BY id;  -- 1,3
SELECT 'plain_scan' AS step, id FROM uk_txt ORDER BY id;  -- 1,3

DROP TABLE uk_txt;
