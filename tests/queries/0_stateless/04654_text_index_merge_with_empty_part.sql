-- Regression test: merging a text index with an empty source part threw `ATTEMPT_TO_READ_AFTER_EOF`.
-- A mutation that deletes all rows of a part leaves the text index files empty (no granules are
-- serialized, so not even the header is written) but still listed in the part's checksums, and
-- `MergeTextIndexesTask` tried to read the header of such a file.

DROP TABLE IF EXISTS t_text_index_merge_with_empty_part;

CREATE TABLE t_text_index_merge_with_empty_part
(
    ts DateTime,
    body String,
    INDEX idx lower(body) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS
    remove_empty_parts = 0, -- keep the empty part until OPTIMIZE
    max_bytes_to_merge_at_max_space_in_pool = 1; -- no background merges before OPTIMIZE

INSERT INTO t_text_index_merge_with_empty_part VALUES ('2026-01-01 00:00:00', 'keeper row timeout');
INSERT INTO t_text_index_merge_with_empty_part VALUES ('2026-01-01 00:00:01', 'doomed row');

-- Turn the second part into an empty part that still carries the (empty) text index files.
ALTER TABLE t_text_index_merge_with_empty_part DELETE WHERE body = 'doomed row' SETTINGS mutations_sync = 2;

SELECT 'parts before optimize';
SELECT rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index_merge_with_empty_part' AND active
ORDER BY name;

OPTIMIZE TABLE t_text_index_merge_with_empty_part FINAL;

SELECT 'parts after optimize';
SELECT rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_index_merge_with_empty_part' AND active
ORDER BY name;

SELECT count() FROM t_text_index_merge_with_empty_part
WHERE hasToken(lower(body), 'keeper')
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM t_text_index_merge_with_empty_part
WHERE hasToken(lower(body), 'doomed')
SETTINGS force_data_skipping_indices = 'idx';

-- A merge where every source part is empty.
ALTER TABLE t_text_index_merge_with_empty_part DELETE WHERE 1 SETTINGS mutations_sync = 2;
OPTIMIZE TABLE t_text_index_merge_with_empty_part FINAL;
SELECT count() FROM t_text_index_merge_with_empty_part;

DROP TABLE t_text_index_merge_with_empty_part;
