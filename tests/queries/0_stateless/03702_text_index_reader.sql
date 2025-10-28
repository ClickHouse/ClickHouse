set allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS default.t;
CREATE TABLE default.t
(
    `id`   UInt64,
    `text` String,
    INDEX inv_idx text TYPE text(tokenizer = 'default') GRANULARITY 4
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 32;
INSERT INTO t VALUES (0,'a'),(1,'b'),(2,'c'),(3,'d');
SELECT id FROM t WHERE hasToken(text, 'b');
SELECT id FROM t WHERE hasToken(text, 'c');
INSERT INTO t SELECT number , 'aaabbbccc' FROM numbers(128);
SELECT id FROM t WHERE hasToken(text, 'aaabbbccc');
