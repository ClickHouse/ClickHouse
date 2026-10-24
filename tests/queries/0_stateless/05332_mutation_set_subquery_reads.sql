-- The set is used only by an assignment, so the affected-rows check cannot build it.
-- Count builds across all keys: alias differences used to register the same subquery twice.
SET mutations_sync = 2;
SET allow_nondeterministic_mutations = 1;

CREATE TABLE mutation_set_source
(
    k UInt64,
    period UInt8,
    payload String
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1024, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

-- `period` is selective by granule but is not in the sorting key. Reading it first
-- must save reads of the wider set keys, rather than relying on primary-key pruning.
INSERT INTO mutation_set_source
SELECT number, intDiv(number, 1024), hex(sipHash128(number)) FROM numbers(16384);

CREATE TABLE mutation_set_target
(
    k UInt64,
    payload String,
    matched UInt8,
    previous_matched UInt8
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

INSERT INTO mutation_set_target
SELECT number * 1024, hex(sipHash128(number * 1024)), 0, 0 FROM numbers(16);

ALTER TABLE mutation_set_target UPDATE
    matched = (k, payload) IN
        (SELECT k, payload FROM mutation_set_source WHERE period = 7 SETTINGS use_query_condition_cache = 0),
    previous_matched = matched + 1
WHERE 1;

-- All assignments see the original row, including the value of another assigned column.
SELECT count(), sum(matched), sum(previous_matched) FROM mutation_set_target;

SYSTEM FLUSH LOGS part_log;
SELECT
    count() = 1 AS one_part,
    sum(ProfileEvents['SetsBuiltFromSubquery']) = 1 AS one_set,
    sum(ProfileEvents['RowsReadByPrewhereReaders']) > 0 AS used_prewhere,
    sum(ProfileEvents['RowsReadByMainReader']) < 8192 AS avoided_full_key_read
FROM system.part_log
WHERE database = currentDatabase() AND table = 'mutation_set_target'
    AND event_type = 'MutatePart' AND error = 0;

-- Explicitly disabling `PREWHERE` in the subquery must still be respected.
CREATE TABLE mutation_set_control AS mutation_set_target;
INSERT INTO mutation_set_control
SELECT number * 1024, hex(sipHash128(number * 1024)), 0, 0 FROM numbers(16);

ALTER TABLE mutation_set_control UPDATE matched = (k, payload) IN
    (SELECT k, payload FROM mutation_set_source WHERE period = 7
        SETTINGS optimize_move_to_prewhere = 0, use_query_condition_cache = 0)
WHERE 1;

SELECT count(), sum(matched) FROM mutation_set_control;

SYSTEM FLUSH LOGS part_log;
SELECT
    count() = 1 AS one_part,
    sum(ProfileEvents['SetsBuiltFromSubquery']) = 1 AS one_set,
    sum(ProfileEvents['RowsReadByPrewhereReaders']) = 0 AS no_prewhere,
    sum(ProfileEvents['RowsReadByMainReader']) >= 16384 AS full_key_read
FROM system.part_log
WHERE database = currentDatabase() AND table = 'mutation_set_control'
    AND event_type = 'MutatePart' AND error = 0;

-- The reported lightweight `DELETE` shape uses an update of the row mask.
DELETE FROM mutation_set_target WHERE (k, payload) IN
    (SELECT k, payload FROM mutation_set_source WHERE period = 7)
SETTINGS lightweight_delete_mode = 'alter_update', lightweight_deletes_sync = 2;
SELECT count(), sum(matched), sum(previous_matched) FROM mutation_set_target;

DROP TABLE mutation_set_target;
DROP TABLE mutation_set_control;
DROP TABLE mutation_set_source;
