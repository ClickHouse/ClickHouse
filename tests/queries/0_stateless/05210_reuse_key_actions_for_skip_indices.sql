-- A computed sorting key or skip index falls back to analyzing the combined expression.
CREATE TABLE key_actions_reuse
(
    team UInt64,
    value UInt64,
    text String,
    payload String DEFAULT concat(text, '!'),
    normalized String ALIAS lower(text),
    INDEX value_idx value TYPE minmax GRANULARITY 1,
    INDEX normalized_idx normalized TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY team
ORDER BY (team, lower(text))
SETTINGS min_bytes_for_full_part_storage = 5368709120;

INSERT INTO key_actions_reuse (team, value, text) SELECT number % 2, number, if(number % 3 = 0, 'ABC', 'ZXY') FROM numbers(1000);
SELECT count(), sum(value), min(normalized) FROM key_actions_reuse;
SELECT count() FROM key_actions_reuse WHERE lower(text) = 'abc';
SELECT count() FROM key_actions_reuse WHERE value = 123 SETTINGS force_data_skipping_indices = 'value_idx';

-- Preserve inputs used only by a computed index, including after metadata changes.
ALTER TABLE key_actions_reuse ADD INDEX length_idx length(payload) TYPE minmax GRANULARITY 1;
INSERT INTO key_actions_reuse (team, value, text) SELECT number % 2, number, 'DEFG' FROM numbers(1000);
SELECT count(), sum(value), sum(length(text)), sum(length(payload)) FROM key_actions_reuse;

-- Only selected indexes must have their expressions prepared.
INSERT INTO key_actions_reuse (team, value, text) SELECT number % 2, number, 'HI' FROM numbers(1000)
SETTINGS exclude_materialize_skip_indexes_on_insert = 'length_idx';
SELECT count(), sum(value), sum(length(text)), sum(length(payload)) FROM key_actions_reuse;
ALTER TABLE key_actions_reuse MATERIALIZE INDEX length_idx SETTINGS mutations_sync = 2;
SELECT count() FROM key_actions_reuse WHERE length(payload) = 3 SETTINGS force_data_skipping_indices = 'length_idx';

CREATE TABLE key_actions_empty (value UInt64, INDEX value_idx value TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_full_part_storage = 5368709120;
INSERT INTO key_actions_empty VALUES (1), (2);
SELECT sum(value) FROM key_actions_empty;
DROP TABLE key_actions_empty;

-- A sorting key and skip indexes over direct columns reuse the prepared actions.
-- Two indexes over the same column must expose the column only once.
CREATE TABLE key_actions_direct
(
    team UInt64,
    value UInt64,
    text String,
    INDEX value_idx value TYPE minmax GRANULARITY 1,
    INDEX text_set_idx text TYPE set(0) GRANULARITY 1,
    INDEX text_bloom_idx text TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY team
SETTINGS index_granularity = 128, min_bytes_for_full_part_storage = 5368709120;

INSERT INTO key_actions_direct SELECT number % 3, number, toString(number % 10) FROM numbers(1000);
SELECT count(), sum(value), uniqExact(text) FROM key_actions_direct;
SELECT count() FROM key_actions_direct WHERE value BETWEEN 100 AND 199 SETTINGS force_data_skipping_indices = 'value_idx';
SELECT count() FROM key_actions_direct WHERE text = '7' SETTINGS force_data_skipping_indices = 'text_set_idx,text_bloom_idx';

INSERT INTO key_actions_direct SELECT number % 3, number + 1000, 'x' FROM numbers(1000)
SETTINGS exclude_materialize_skip_indexes_on_insert = 'text_bloom_idx';
SELECT count(), sum(value) FROM key_actions_direct;
SELECT count() FROM key_actions_direct WHERE text = 'x' SETTINGS force_data_skipping_indices = 'text_set_idx';
DROP TABLE key_actions_direct;
DROP TABLE key_actions_reuse;
