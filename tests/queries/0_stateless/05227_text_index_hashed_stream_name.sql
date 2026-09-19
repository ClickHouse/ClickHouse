-- The index name is longer than max_file_name_length, so every text index substream is stored
-- under a sipHash128String name. Nothing in the tree covered a hashed text-index stream name,
-- nor a text index on packed part storage, where a substream's size is recorded by the packed
-- archive index rather than by the filesystem.

-- The two plans this exercises are each gated by several settings that the runner randomizes
-- (QueryPlanOptimizationSettings.cpp: the count rewrite needs query_plan_direct_read_from_text_index
-- AND use_skip_indexes AND query_plan_optimize_count_from_text_index AND optimize_trivial_count_query),
-- so pin them exactly as 02346_text_index_optimize_trivial_count.sql does.
SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_trivial_count_query = 1;
SET query_plan_optimize_count_from_text_index = 1;
SET max_rows_to_group_by = 0;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
-- Parallel replicas disarm both plans: the count rewrite bails on canUseParallelReplicasOnInitiator,
-- and with parallel_replicas_local_plan = 0 the read plan holds no local part read to inspect.
SET enable_parallel_replicas = 0;
SET enable_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET text_index_hint_max_selectivity = 1.;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_hashed_full;
DROP TABLE IF EXISTS t_hashed_packed;
DROP TABLE IF EXISTS t_hashed_positions;
DROP TABLE IF EXISTS t_plain_control;

-- Hashed substream names on full (per-file) part storage.
CREATE TABLE t_hashed_full
(
    id UInt64,
    message String,
    INDEX a_very_long_text_index_name_that_must_be_hashed_when_stored_per_file message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS replace_long_file_name_to_hash = 1,
         max_file_name_length = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         remove_empty_parts = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Same index name, but the part storage is packed, so getFileSize answers from the archive index.
CREATE TABLE t_hashed_packed
(
    id UInt64,
    message String,
    INDEX a_very_long_text_index_name_that_must_be_hashed_when_stored_per_file message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS replace_long_file_name_to_hash = 1,
         max_file_name_length = 64,
         min_bytes_for_full_part_storage = '100G',
         remove_empty_parts = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Positions substream (only written with support_phrase_search), read by a third stream-building
-- site. Its blob offsets are validated against the stream size, so this is the one arm that
-- reports a wrong size instead of absorbing it; posting_list_block_size is lowered so postings
-- span many blocks.
CREATE TABLE t_hashed_positions
(
    id UInt64,
    message String,
    INDEX a_very_long_text_index_name_that_must_be_hashed_when_stored_per_file message TYPE text(tokenizer = 'splitByNonAlpha', support_phrase_search = 1, posting_list_block_size = 256) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS allow_experimental_text_index_phrase_search = 1,
         replace_long_file_name_to_hash = 1,
         max_file_name_length = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         remove_empty_parts = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Control with a short index name, so a failure localises to name hashing. The runner randomizes
-- max_file_name_length down to 0, which hashes names of any length, so the flag is pinned not implied.
CREATE TABLE t_plain_control
(
    id UInt64,
    message String,
    INDEX idx message TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS replace_long_file_name_to_hash = 0,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         max_bytes_to_merge_at_max_space_in_pool = 1;

-- Two parts per table, so that deleting the first part's rows below leaves an empty part whose
-- text index substreams are 0 bytes and still listed in checksums, on both storage types.
INSERT INTO t_hashed_full SELECT number, concat('alpha bravo w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_hashed_full SELECT number + 2000, concat('charlie delta w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_hashed_packed SELECT number, concat('alpha bravo w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_hashed_packed SELECT number + 2000, concat('charlie delta w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_hashed_positions SELECT number, concat('alpha bravo w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_hashed_positions SELECT number + 2000, concat('charlie delta w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_plain_control SELECT number, concat('alpha bravo w', toString(number % 500)) FROM numbers(2000);
INSERT INTO t_plain_control SELECT number + 2000, concat('charlie delta w', toString(number % 500)) FROM numbers(2000);

SELECT 'storage types';
SELECT table, countDistinct(part_storage_type), any(part_storage_type) FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_hashed_full', 't_hashed_packed') AND active
GROUP BY table ORDER BY table;

SELECT 'index materialized', countIf(secondary_indices_compressed_bytes > 0) FROM system.parts
WHERE database = currentDatabase() AND table = 't_hashed_full' AND active;

-- Both stream-building plans must really be taken, or every oracle below is vacuous.
SELECT 'count path taken', count() FROM (EXPLAIN SELECT count() FROM t_hashed_full WHERE hasToken(message, 'alpha'))
WHERE explain ILIKE '%ReadFromTextIndexCount%';
-- count() > 0, not the count: the marker appears once or twice depending on how many prewhere
-- read steps the plan splits into, which the runner's random settings change.
SELECT 'read path taken', count() > 0 FROM (EXPLAIN SELECT id, message FROM t_hashed_full WHERE hasToken(message, 'w17'))
WHERE explain ILIKE '%\_\_text\_index\_%';

-- Each row pins the value AND its agreement with the same query answered without the index,
-- so neither a wrong shared answer nor a trivially equal pair of zeros can pass.
SELECT 'count hasToken', 'full',
    (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'alpha')) AS c,
    c = (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'alpha') SETTINGS use_skip_indexes = 0);
SELECT 'count hasToken', 'packed',
    (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'alpha')) AS c,
    c = (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'alpha') SETTINGS use_skip_indexes = 0);
SELECT 'count hasToken', 'control',
    (SELECT count() FROM t_plain_control WHERE hasToken(message, 'alpha')) AS c,
    c = (SELECT count() FROM t_plain_control WHERE hasToken(message, 'alpha') SETTINGS use_skip_indexes = 0);

SELECT 'count hasAllTokens', 'full',
    (SELECT count() FROM t_hashed_full WHERE hasAllTokens(message, ['alpha', 'w17'])) AS c,
    c = (SELECT count() FROM t_hashed_full WHERE hasAllTokens(message, ['alpha', 'w17']) SETTINGS use_skip_indexes = 0);
SELECT 'count hasAllTokens', 'packed',
    (SELECT count() FROM t_hashed_packed WHERE hasAllTokens(message, ['alpha', 'w17'])) AS c,
    c = (SELECT count() FROM t_hashed_packed WHERE hasAllTokens(message, ['alpha', 'w17']) SETTINGS use_skip_indexes = 0);

-- The reader path (MergeTreeReaderTextIndex) builds more streams per part than the count path.
SELECT 'read hasToken', 'full',
    (SELECT sum(cityHash64(id, message)) FROM t_hashed_full WHERE hasToken(message, 'w17')) AS h,
    h = (SELECT sum(cityHash64(id, message)) FROM t_hashed_full WHERE hasToken(message, 'w17') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'w17'));
SELECT 'read hasToken', 'packed',
    (SELECT sum(cityHash64(id, message)) FROM t_hashed_packed WHERE hasToken(message, 'w17')) AS h,
    h = (SELECT sum(cityHash64(id, message)) FROM t_hashed_packed WHERE hasToken(message, 'w17') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'w17'));
SELECT 'read hasToken', 'control',
    (SELECT sum(cityHash64(id, message)) FROM t_plain_control WHERE hasToken(message, 'w17')) AS h,
    h = (SELECT sum(cityHash64(id, message)) FROM t_plain_control WHERE hasToken(message, 'w17') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM t_plain_control WHERE hasToken(message, 'w17'));

-- Phrases whose second token is late in the dictionary, so the positions blob they need lies deep
-- in the stream rather than at its start.
SELECT 'phrase', 'early',
    (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'alpha bravo')) AS c,
    c = (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'alpha bravo') SETTINGS use_skip_indexes = 0);
SELECT 'phrase', 'late',
    (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'bravo w99')) AS c,
    c = (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'bravo w99') SETTINGS use_skip_indexes = 0);
SELECT 'phrase', 'latest',
    (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'delta w499')) AS c,
    c = (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'delta w499') SETTINGS use_skip_indexes = 0);

-- Deleting every row of one part leaves its text index substreams at 0 bytes and still listed in
-- the part's checksums, so a size-based branch would send exactly this part back to the filesystem.
ALTER TABLE t_hashed_full DELETE WHERE id < 2000 SETTINGS mutations_sync = 2;
ALTER TABLE t_hashed_packed DELETE WHERE id < 2000 SETTINGS mutations_sync = 2;
ALTER TABLE t_hashed_positions DELETE WHERE id < 2000 SETTINGS mutations_sync = 2;

SELECT 'empty part kept', table, countIf(rows = 0), countDistinct(part_storage_type), any(part_storage_type)
FROM system.parts
WHERE database = currentDatabase() AND table IN ('t_hashed_full', 't_hashed_packed') AND active
GROUP BY table ORDER BY table;

SELECT 'count surviving', 'full',
    (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'charlie')) AS c,
    c = (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'charlie') SETTINGS use_skip_indexes = 0);
SELECT 'count surviving', 'packed',
    (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'charlie')) AS c,
    c = (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'charlie') SETTINGS use_skip_indexes = 0);

SELECT 'count deleted', 'full',
    (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'alpha')) AS c,
    c = (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'alpha') SETTINGS use_skip_indexes = 0);
SELECT 'count deleted', 'packed',
    (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'alpha')) AS c,
    c = (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'alpha') SETTINGS use_skip_indexes = 0);

SELECT 'read after delete', 'full',
    (SELECT sum(cityHash64(id, message)) FROM t_hashed_full WHERE hasToken(message, 'w17')) AS h,
    h = (SELECT sum(cityHash64(id, message)) FROM t_hashed_full WHERE hasToken(message, 'w17') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM t_hashed_full WHERE hasToken(message, 'w17'));
SELECT 'read after delete', 'packed',
    (SELECT sum(cityHash64(id, message)) FROM t_hashed_packed WHERE hasToken(message, 'w17')) AS h,
    h = (SELECT sum(cityHash64(id, message)) FROM t_hashed_packed WHERE hasToken(message, 'w17') SETTINGS use_skip_indexes = 0),
    (SELECT count() FROM t_hashed_packed WHERE hasToken(message, 'w17'));

SELECT 'phrase after delete', 'late',
    (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'delta w499')) AS c,
    c = (SELECT count() FROM t_hashed_positions WHERE hasPhrase(message, 'delta w499') SETTINGS use_skip_indexes = 0);

SELECT 'check full';
CHECK TABLE t_hashed_full SETTINGS check_query_single_value_result = 1;
SELECT 'check packed';
CHECK TABLE t_hashed_packed SETTINGS check_query_single_value_result = 1;
SELECT 'check positions';
CHECK TABLE t_hashed_positions SETTINGS check_query_single_value_result = 1;
SELECT 'check control';
CHECK TABLE t_plain_control SETTINGS check_query_single_value_result = 1;

DROP TABLE t_hashed_full;
DROP TABLE t_hashed_packed;
DROP TABLE t_hashed_positions;
DROP TABLE t_plain_control;
