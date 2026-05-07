SET allow_experimental_analyzer = 1;

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

-- Two ALIAS columns with different lambda bodies over the same arguments.
-- The canonical matching must not confuse them.
DROP TABLE IF EXISTS tab2;

CREATE TABLE tab2
(
    user_id UInt64,
    color_map Map(String, String),
    kv_eq Array(String) ALIAS arrayMap((k, v) -> concat(k, '=', v), mapKeys(color_map), mapValues(color_map)),
    kv_colon Array(String) ALIAS arrayMap((k, v) -> concat(k, ':', v), mapKeys(color_map), mapValues(color_map)),
    INDEX idx_kv_eq kv_eq TYPE text(tokenizer = 'array'),
    INDEX idx_kv_colon kv_colon TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO tab2 VALUES (1, {'a': 'b'});

SELECT user_id FROM tab2 WHERE has(kv_eq, 'a=b') ORDER BY user_id SETTINGS force_data_skipping_indices = 'idx_kv_eq';
SELECT user_id FROM tab2 WHERE has(kv_colon, 'a:b') ORDER BY user_id SETTINGS force_data_skipping_indices = 'idx_kv_colon';

DROP TABLE tab2;
