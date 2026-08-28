SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS t_json_all_values_array_prefix_suffix;

CREATE TABLE t_json_all_values_array_prefix_suffix
(
    data JSON(tags Array(String)),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_array_prefix_suffix
SELECT '{"tags":["a","b"]}' FROM numbers(4);
INSERT INTO t_json_all_values_array_prefix_suffix
SELECT '{"tags":["c","d"]}' FROM numbers(4);

SELECT count() FROM t_json_all_values_array_prefix_suffix WHERE startsWith(data.tags, ['a']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_json_all_values_array_prefix_suffix WHERE startsWith(data.tags, ['a']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_json_all_values_array_prefix_suffix WHERE endsWith(data.tags, ['b']) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_json_all_values_array_prefix_suffix WHERE endsWith(data.tags, ['b']) SETTINGS use_skip_indexes = 0;

DROP TABLE t_json_all_values_array_prefix_suffix;
