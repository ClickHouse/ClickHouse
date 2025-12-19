DROP TABLE IF EXISTS t_text_index_replacing;

SET allow_experimental_full_text_index = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_query_condition_cache = 0;

CREATE TABLE t_text_index_replacing
(
    id UInt64,
    version UInt64,
    text String,
    INDEX idx_text (text) TYPE text(tokenizer = array)
)
ENGINE = ReplacingMergeTree(version) ORDER BY id;

INSERT INTO t_text_index_replacing SELECT number, 1, 'v' || toString(number) FROM numbers(100000);
INSERT INTO t_text_index_replacing SELECT number, 2, 'v' || toString(number) || '_updated' FROM numbers(0, 100000, 3);

OPTIMIZE TABLE t_text_index_replacing FINAL;

SELECT count() FROM t_text_index_replacing WHERE text = 'v12345';
SELECT count() FROM t_text_index_replacing WHERE text = 'v12345_updated';

SELECT count() FROM t_text_index_replacing FINAL WHERE text = 'v54320';
SELECT count() FROM t_text_index_replacing WHERE text = 'v54320_updated';

DROP TABLE t_text_index_replacing;
