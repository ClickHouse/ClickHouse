-- Tags: no-fasttest
-- no-fasttest: AES_128_GCM_SIV depends on OpenSSL.

-- A dictionary codec must never replace an encrypting part default with a non-encrypting codec:
-- the dictionary stores the indexed tokens, so that would write user data in plaintext.
--
-- The assertions compare arms inside one run, so they stay valid under randomized merge-tree
-- settings and no tag is needed for them. Verified over 100 runs with randomization enabled.

DROP TABLE IF EXISTS t_enc_inherit;
DROP TABLE IF EXISTS t_enc_override;
DROP TABLE IF EXISTS t_enc_explicit;
DROP TABLE IF EXISTS t_plain_inherit;
DROP TABLE IF EXISTS t_plain_zstd;
DROP TABLE IF EXISTS t_enc_temp;
DROP TABLE IF EXISTS t_plain_temp;

CREATE TABLE t_enc_inherit (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4, AES_128_GCM_SIV', text_index_dictionary_compression_codec = '';

CREATE TABLE t_enc_override (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4, AES_128_GCM_SIV', text_index_dictionary_compression_codec = 'LZ4';

CREATE TABLE t_enc_explicit (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4, AES_128_GCM_SIV', text_index_dictionary_compression_codec = 'ZSTD(1), AES_128_GCM_SIV';

CREATE TABLE t_plain_inherit (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4', text_index_dictionary_compression_codec = '';

CREATE TABLE t_plain_zstd (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4', text_index_dictionary_compression_codec = 'ZSTD(3)';

INSERT INTO t_enc_inherit   SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(50000);
INSERT INTO t_enc_override  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(50000);
INSERT INTO t_enc_explicit  SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(50000);
INSERT INTO t_plain_inherit SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(50000);
INSERT INTO t_plain_zstd    SELECT concat('tok', toString(number), ' the quick brown fox') FROM numbers(50000);

OPTIMIZE TABLE t_enc_inherit FINAL;
OPTIMIZE TABLE t_enc_override FINAL;
OPTIMIZE TABLE t_enc_explicit FINAL;
OPTIMIZE TABLE t_plain_inherit FINAL;
OPTIMIZE TABLE t_plain_zstd FINAL;

-- AES_128_GCM_SIV derives its IV from the data, so these sizes are stable between runs.
SELECT 'guard';
WITH sizes AS
(
    SELECT table, sum(data_compressed_bytes) AS c
    FROM system.data_skipping_indices
    WHERE database = currentDatabase()
      AND table IN ('t_enc_inherit', 't_enc_override', 't_enc_explicit', 't_plain_inherit', 't_plain_zstd')
    GROUP BY table
)
SELECT
    -- The guard refused to replace the encrypting default with plain LZ4.
    (SELECT c FROM sizes WHERE table = 't_enc_override') = (SELECT c FROM sizes WHERE table = 't_enc_inherit') AS refused_downgrade,
    -- The encrypted dictionary really carries per block nonce and tag bytes, so the assertion
    -- above is not comparing two plaintext files.
    (SELECT c FROM sizes WHERE table = 't_enc_inherit') > (SELECT c FROM sizes WHERE table = 't_plain_inherit') AS encryption_has_overhead,
    -- The substitution mechanism works at all in a table that does not encrypt, so the first
    -- assertion cannot pass by the setting being ignored everywhere.
    (SELECT c FROM sizes WHERE table = 't_plain_zstd') != (SELECT c FROM sizes WHERE table = 't_plain_inherit') AS mechanism_is_live,
    -- An explicitly encrypting dictionary codec is honoured: the rule is no downgrade, not
    -- "ignore the setting whenever the default encrypts".
    (SELECT c FROM sizes WHERE table = 't_enc_explicit') != (SELECT c FROM sizes WHERE table = 't_enc_inherit') AS encrypting_codec_honoured,
    -- No arm passed the assertions above by writing no index at all.
    (SELECT min(c) FROM sizes) > 0 AS all_arms_wrote_a_dictionary;

SELECT 'reads';
SELECT count(), countIf(hasToken(s, 'tok123')), countIf(s LIKE '%tok12345%') FROM t_enc_inherit;
SELECT count(), countIf(hasToken(s, 'tok123')), countIf(s LIKE '%tok12345%') FROM t_enc_override;
SELECT count(), countIf(hasToken(s, 'tok123')), countIf(s LIKE '%tok12345%') FROM t_enc_explicit;

SELECT 'check table';
CHECK TABLE t_enc_inherit SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_enc_override SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_enc_explicit SETTINGS check_query_single_value_result = 1;

-- A plaintext part default with an encrypting dictionary codec: the one combination where the
-- dictionary codec encrypts and the part default does not, so it is also the combination in which
-- a temporary segment written with the part default would put the indexed tokens on disk in
-- plaintext.
--
-- A merge rebuilds a text index, and so writes temporary segments, only when a source part does
-- not already carry it, hence materialize_skip_indexes_on_insert = 0 below. The low flush
-- threshold additionally forces mid-stream flushes rather than the single one at end of input.
-- max_bytes_to_merge_at_max_space_in_pool = 1 leaves OPTIMIZE FINAL (which ignores that limit) as
-- the only merger, so no background merge can void the temporary-segment assertion, and
-- optimize_throw_if_noop = 1 fails a no-op OPTIMIZE loudly; both as in 04546_text_index_merge_fsync.

CREATE TABLE t_enc_temp (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4', text_index_dictionary_compression_codec = 'LZ4, AES_128_GCM_SIV',
         text_index_max_processed_tokens_before_flush = 50000, max_bytes_to_merge_at_max_space_in_pool = 1;

CREATE TABLE t_plain_temp (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS default_compression_codec = 'LZ4', text_index_dictionary_compression_codec = '',
         text_index_max_processed_tokens_before_flush = 50000, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_enc_temp   SELECT concat('tok', toString(number), ' the quick brown fox')         FROM numbers(50000) SETTINGS materialize_skip_indexes_on_insert = 0;
INSERT INTO t_enc_temp   SELECT concat('tok', toString(number + 50000), ' the quick brown fox') FROM numbers(50000) SETTINGS materialize_skip_indexes_on_insert = 0;
INSERT INTO t_plain_temp SELECT concat('tok', toString(number), ' the quick brown fox')         FROM numbers(50000) SETTINGS materialize_skip_indexes_on_insert = 0;
INSERT INTO t_plain_temp SELECT concat('tok', toString(number + 50000), ' the quick brown fox') FROM numbers(50000) SETTINGS materialize_skip_indexes_on_insert = 0;

OPTIMIZE TABLE t_enc_temp FINAL SETTINGS log_comment = '05239_temp_enc', optimize_throw_if_noop = 1;
OPTIMIZE TABLE t_plain_temp FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'temp segments';
SYSTEM FLUSH LOGS query_log;
WITH sizes AS
(
    SELECT table, sum(data_compressed_bytes) AS c
    FROM system.data_skipping_indices
    WHERE database = currentDatabase()
      AND table IN ('t_enc_temp', 't_plain_temp')
    GROUP BY table
)
SELECT
    -- The encrypting dictionary codec is honoured over a plaintext part default.
    (SELECT c FROM sizes WHERE table = 't_enc_temp') != (SELECT c FROM sizes WHERE table = 't_plain_temp') AS plain_default_honours_encrypting_codec,
    -- Neither arm passed the assertion above by writing no index at all.
    (SELECT min(c) FROM sizes) > 0 AS both_temp_arms_wrote_a_dictionary,
    -- The merge really went through the temporary-segment path, so the assertions here and the
    -- read-back below are not vacuous.
    (
        SELECT ProfileEvents['TextIndexTemporarySegmentsWritten']
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND current_database = currentDatabase() AND log_comment = '05239_temp_enc'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    ) >= 1 AS temporary_segment_was_written;

-- A temporary segment the consuming merge could not decode would fail both of these.
SELECT 'temp reads';
SELECT count(), countIf(hasToken(s, 'tok123')), countIf(s LIKE '%tok12345%') FROM t_enc_temp;
SELECT count(), countIf(hasToken(s, 'tok123')), countIf(s LIKE '%tok12345%') FROM t_plain_temp;

SELECT 'temp check table';
CHECK TABLE t_enc_temp SETTINGS check_query_single_value_result = 1;
CHECK TABLE t_plain_temp SETTINGS check_query_single_value_result = 1;

DROP TABLE t_enc_inherit;
DROP TABLE t_enc_override;
DROP TABLE t_enc_explicit;
DROP TABLE t_plain_inherit;
DROP TABLE t_plain_zstd;
DROP TABLE t_enc_temp;
DROP TABLE t_plain_temp;
