-- Tags: no-parallel-replicas
-- no-parallel-replicas: the assertions below read ProfileEvents of the initiator query.
-- Tests that a text index dictionary scan for LIKE/ILIKE patterns visits only the dictionary blocks an
-- anchored pattern can match, and that neither the narrowed block range nor the filtering of a block's
-- tokens by the pattern's mandatory literal changes which rows are returned. Every scenario is compared
-- against the same query with use_skip_indexes = 0.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET text_index_like_min_pattern_length = 4;
SET text_index_like_max_postings_to_read = 100000;
SET use_query_condition_cache = 0;
SET optimize_rewrite_like_perfect_affix = 0;
SET max_threads = 1;
SET log_queries = 1;
SET log_profile_events = 1;

DROP TABLE IF EXISTS tab;

-- dictionary_block_size = 4 with the 14 tokens below gives 4 dictionary blocks whose first tokens are
-- 'toaa0', 'toaa4', 'tocc0' and 'toxx', so the block a pattern range covers is known exactly.
-- 'toaa%' spans a block boundary, 'toxx' is a strict prefix of 'toxxy', and 'aaaa' sorts below every block.
CREATE TABLE tab
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab VALUES
    (1, 'toaa0'), (2, 'toaa1'), (3, 'toaa2'), (4, 'toaa3'), (5, 'toaa4'), (6, 'toaa5'),
    (7, 'tobb0'), (8, 'tobb1'), (9, 'tocc0'), (10, 'tocc1'), (11, 'toxx'), (12, 'toxxy'),
    (13, 'toaa0 tobb0');
-- Two tokens with more postings than MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS, so that the
-- text_index_like_max_postings_to_read budget below is actually charged.
INSERT INTO tab SELECT 100 + number, concat('todd', toString(number % 2)) FROM numbers(16);
OPTIMIZE TABLE tab FINAL;

SELECT 'prefix present', groupArray(id) FROM tab WHERE message LIKE 'tobb%' SETTINGS log_comment = '05218_prefix';
SELECT 'prefix present, no index', groupArray(id) FROM tab WHERE message LIKE 'tobb%' SETTINGS use_skip_indexes = 0;

SELECT 'prefix absent', groupArray(id) FROM tab WHERE message LIKE 'aaaa%' SETTINGS log_comment = '05218_absent';
SELECT 'prefix absent, no index', groupArray(id) FROM tab WHERE message LIKE 'aaaa%' SETTINGS use_skip_indexes = 0;

SELECT 'prefix over a block boundary', groupArray(id) FROM tab WHERE message LIKE 'toaa%' SETTINGS log_comment = '05218_boundary';
SELECT 'prefix over a block boundary, no index', groupArray(id) FROM tab WHERE message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

SELECT 'prefix of another token', groupArray(id) FROM tab WHERE message LIKE 'toxx%' SETTINGS log_comment = '05218_strict_prefix';
SELECT 'prefix of another token, no index', groupArray(id) FROM tab WHERE message LIKE 'toxx%' SETTINGS use_skip_indexes = 0;

SELECT 'infix', groupArray(id) FROM tab WHERE message LIKE '%toaa0%' SETTINGS log_comment = '05218_infix';
SELECT 'infix, no index', groupArray(id) FROM tab WHERE message LIKE '%toaa0%' SETTINGS use_skip_indexes = 0;

SELECT 'suffix', groupArray(id) FROM tab WHERE message LIKE '%tobb0' SETTINGS log_comment = '05218_suffix';
SELECT 'suffix, no index', groupArray(id) FROM tab WHERE message LIKE '%tobb0' SETTINGS use_skip_indexes = 0;

SELECT 'two prefixes', groupArray(id) FROM tab WHERE message LIKE 'toaa%' OR message LIKE 'tobb%' SETTINGS log_comment = '05218_two_prefixes';
SELECT 'two prefixes, no index', groupArray(id) FROM tab WHERE message LIKE 'toaa%' OR message LIKE 'tobb%' SETTINGS use_skip_indexes = 0;

SELECT 'prefix and infix', groupArray(id) FROM tab WHERE message LIKE 'toaa%' AND message LIKE '%toaa0%' SETTINGS log_comment = '05218_prefix_infix';
SELECT 'prefix and infix, no index', groupArray(id) FROM tab WHERE message LIKE 'toaa%' AND message LIKE '%toaa0%' SETTINGS use_skip_indexes = 0;

-- No ILIKE needle here may contain 'k' or 'K': such a needle is refused before the scan starts, because
-- U+212A folds onto 'k', so the scenario would assert nothing about the dictionary scan.
SELECT 'ilike infix', groupArray(id) FROM tab WHERE message ILIKE '%TOAA0%' SETTINGS log_comment = '05218_ilike';
SELECT 'ilike infix, no index', groupArray(id) FROM tab WHERE message ILIKE '%TOAA0%' SETTINGS use_skip_indexes = 0;

SELECT 'postings budget exhausted', groupArray(id) FROM tab WHERE message LIKE '%todd%' SETTINGS log_comment = '05218_budget', text_index_like_max_postings_to_read = 1;
SELECT 'postings budget exhausted, no index', groupArray(id) FROM tab WHERE message LIKE '%todd%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS pattern_scan_discarded,
    read_rows < (SELECT count() FROM tab) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05218_prefix', '05218_absent', '05218_boundary', '05218_strict_prefix', '05218_infix',
                      '05218_suffix', '05218_two_prefixes', '05218_prefix_infix', '05218_ilike', '05218_budget')
ORDER BY log_comment;

DROP TABLE tab;

SELECT 'Array tokenizer';

DROP TABLE IF EXISTS tab_array;

CREATE TABLE tab_array
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_array VALUES
    (1, 'toaa0'), (2, 'toaa1'), (3, 'toaa2'), (4, 'toaa3'), (5, 'toaa4'), (6, 'toaa5'),
    (7, 'tobb0'), (8, 'tobb1'), (9, 'abcd'), (10, 'abcde');
INSERT INTO tab_array VALUES (11, unhex('FFFFFFFF')), (12, concat(unhex('FFFFFFFF'), 'z'));
OPTIMIZE TABLE tab_array FINAL;

-- The array tokenizer keeps the whole value as one token, so a wildcard-free LIKE compiles to '^literal$'
-- and can only be held by the single dictionary block that literal falls into.
SELECT 'exact', groupArray(id) FROM tab_array WHERE message LIKE 'abcd' SETTINGS log_comment = '05218_array_exact';
SELECT 'exact, no index', groupArray(id) FROM tab_array WHERE message LIKE 'abcd' SETTINGS use_skip_indexes = 0;

SELECT 'arbitrary pattern', groupArray(id) FROM tab_array WHERE message LIKE 'toaa%1' SETTINGS log_comment = '05218_array_arbitrary';
SELECT 'arbitrary pattern, no index', groupArray(id) FROM tab_array WHERE message LIKE 'toaa%1' SETTINGS use_skip_indexes = 0;

-- The prefix has no upper bound, so its range must reach the end of the dictionary.
SELECT 'all-0xFF prefix', groupArray(id) FROM tab_array WHERE message LIKE concat(unhex('FFFFFFFF'), '%') SETTINGS log_comment = '05218_array_max_prefix';
SELECT 'all-0xFF prefix, no index', groupArray(id) FROM tab_array WHERE message LIKE concat(unhex('FFFFFFFF'), '%') SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05218_array_exact', '05218_array_arbitrary', '05218_array_max_prefix')
ORDER BY log_comment;

DROP TABLE tab_array;

SELECT 'Two parts, LowCardinality and Nullable';

DROP TABLE IF EXISTS tab_parts;

CREATE TABLE tab_parts
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_parts VALUES (1, 'toaa0'), (2, 'toaa1'), (3, 'tobb0'), (4, 'tobb1');
INSERT INTO tab_parts VALUES (5, 'toaa2'), (6, 'tocc0'), (7, 'tocc1'), (8, 'toxx');

SELECT 'two parts', groupArray(id) FROM tab_parts WHERE message LIKE 'toaa%';
SELECT 'two parts, no index', groupArray(id) FROM tab_parts WHERE message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab_parts;

DROP TABLE IF EXISTS tab_lc;

CREATE TABLE tab_lc
(
    id UInt32,
    message LowCardinality(String),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_lc VALUES (1, 'toaa0'), (2, 'toaa1'), (3, 'tobb0'), (4, 'tobb1'), (5, 'tocc0'), (6, 'toxx');
OPTIMIZE TABLE tab_lc FINAL;

SELECT 'low cardinality', groupArray(id) FROM tab_lc WHERE message LIKE 'toaa%';
SELECT 'low cardinality, no index', groupArray(id) FROM tab_lc WHERE message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab_lc;

DROP TABLE IF EXISTS tab_nullable;

CREATE TABLE tab_nullable
(
    id UInt32,
    message Nullable(String),
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_nullable VALUES (1, 'toaa0'), (2, 'toaa1'), (3, NULL), (4, 'tobb1'), (5, 'tocc0'), (6, 'toxx');
OPTIMIZE TABLE tab_nullable FINAL;

-- Affix patterns are disabled for a nullable column, so only the infix path reaches the dictionary scan.
SELECT 'nullable infix', groupArray(id) FROM tab_nullable WHERE message LIKE '%toaa0%';
SELECT 'nullable infix, no index', groupArray(id) FROM tab_nullable WHERE message LIKE '%toaa0%' SETTINGS use_skip_indexes = 0;

DROP TABLE tab_nullable;

SELECT 'Straddling literal occurrence';

DROP TABLE IF EXISTS tab_straddle;

-- The first dictionary block holds 'aabb', 'ccdd', 'xxbbcc', 'yybb' as the bytes 'aabbccddxxbbccyybb', so
-- the needle 'bbcc' occurs there twice: at offset 2 it straddles 'aabb' and 'ccdd', at offset 10 it ends
-- exactly at the end of 'xxbbcc'. Only the latter lies inside one token, so only 'xxbbcc' may match.
CREATE TABLE tab_straddle
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_straddle VALUES (1, 'aabb'), (2, 'ccdd'), (3, 'xxbbcc'), (4, 'yybb');
-- Further rows, all sorting after the tokens above, so that a granule is pruned and the first block's layout stays.
INSERT INTO tab_straddle SELECT 100 + number, concat('zz', toString(number)) FROM numbers(16);
OPTIMIZE TABLE tab_straddle FINAL;

SELECT 'straddling then contained', groupArray(id) FROM tab_straddle WHERE message LIKE '%bbcc%' SETTINGS log_comment = '05218_straddle';
SELECT 'straddling then contained, no index', groupArray(id) FROM tab_straddle WHERE message LIKE '%bbcc%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    read_rows < (SELECT count() FROM tab_straddle) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '05218_straddle';

DROP TABLE tab_straddle;

SELECT 'Empty mandatory literal';

DROP TABLE IF EXISTS tab_no_literal;

-- Every literal run of 'a%b%c%d' is one byte, below the three the regexp analysis needs, so the compiled
-- pattern has no mandatory literal and can match a token anywhere in the dictionary; the array tokenizer
-- admits it by counting the four non-wildcard characters. Block first tokens: 'a1b1c1d', 'a5b5c5d', 'tocc0'.
CREATE TABLE tab_no_literal
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = array, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_no_literal VALUES
    (1, 'a1b1c1d'), (2, 'a2b2c2d'), (3, 'a3b3c3d'), (4, 'a4b4c4d'),
    (5, 'a5b5c5d'), (6, 'toaa0'), (7, 'toaa1'), (8, 'tobb0'),
    (9, 'tocc0'), (10, 'todd0'), (11, 'toee0'), (12, 'toff0');
OPTIMIZE TABLE tab_no_literal FINAL;

SELECT 'no mandatory literal', groupArray(id) FROM tab_no_literal WHERE message LIKE 'a%b%c%d' SETTINGS log_comment = '05218_no_literal';
SELECT 'no mandatory literal, no index', groupArray(id) FROM tab_no_literal WHERE message LIKE 'a%b%c%d' SETTINGS use_skip_indexes = 0;

SELECT 'no mandatory literal with a prefix', groupArray(id) FROM tab_no_literal WHERE message LIKE 'a%b%c%d' OR message LIKE 'toaa%' SETTINGS log_comment = '05218_no_literal_mixed';
SELECT 'no mandatory literal with a prefix, no index', groupArray(id) FROM tab_no_literal WHERE message LIKE 'a%b%c%d' OR message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

-- That same prefix on its own is narrowed to one block, so the full count above is the all-or-nothing decline.
SELECT 'prefix alone', groupArray(id) FROM tab_no_literal WHERE message LIKE 'toaa%' SETTINGS log_comment = '05218_no_literal_prefix';
SELECT 'prefix alone, no index', groupArray(id) FROM tab_no_literal WHERE message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    read_rows < (SELECT count() FROM tab_no_literal) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('05218_no_literal', '05218_no_literal_mixed', '05218_no_literal_prefix')
ORDER BY log_comment;

DROP TABLE tab_no_literal;

SELECT 'Block starting at the exclusive upper bound';

DROP TABLE IF EXISTS tab_upper_bound;

-- Block first tokens: 'toaa0' and 'toab'. The range of 'toaa%' ends, exclusively, at 'toab', so the second
-- block starts exactly at that bound and none of its tokens can carry the prefix: one block, not two.
CREATE TABLE tab_upper_bound
(
    id UInt32,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, dictionary_block_size = 4) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 4;

INSERT INTO tab_upper_bound VALUES
    (1, 'toaa0'), (2, 'toaa1'), (3, 'toaa2'), (4, 'toaa3'), (5, 'toab'), (6, 'toab1'), (7, 'toab2'), (8, 'toab3');
OPTIMIZE TABLE tab_upper_bound FINAL;

SELECT 'block starts at the upper bound', groupArray(id) FROM tab_upper_bound WHERE message LIKE 'toaa%' SETTINGS log_comment = '05218_at_upper_bound';
SELECT 'block starts at the upper bound, no index', groupArray(id) FROM tab_upper_bound WHERE message LIKE 'toaa%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['TextIndexReadDictionaryBlocks'] AS dictionary_blocks_read,
    read_rows < (SELECT count() FROM tab_upper_bound) AS granules_pruned
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '05218_at_upper_bound';

DROP TABLE tab_upper_bound;
