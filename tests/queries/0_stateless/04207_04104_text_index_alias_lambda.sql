DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    user_id UInt64,
    color_map Map(String, String),
    colors_kv Array(String) ALIAS arrayMap((k, v) -> concat(k, '=', v), mapKeys(color_map), mapValues(color_map)),
    INDEX idx_colors_kv colors_kv TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO tab VALUES (1, {'favorite': 'red', 'second': 'blue'});
INSERT INTO tab VALUES (2, {'favorite': 'green', 'second': 'blue'});

-- The index must be usable; force_data_skipping_indices would throw if not.
SELECT user_id FROM tab WHERE has(colors_kv, 'favorite=red') ORDER BY user_id SETTINGS force_data_skipping_indices = 'idx_colors_kv';
SELECT user_id FROM tab WHERE has(colors_kv, 'second=blue') ORDER BY user_id SETTINGS force_data_skipping_indices = 'idx_colors_kv';

DROP TABLE tab;
