-- { echoOn }
SELECT arrayStringConcat(tokens('Hello, world!', 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens('错误503', 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens('taichi张三丰in the house', 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens($$don't stop 3,14 can't_stop$$, 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens('안녕하세요 세계', 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens('hello，world', 'asciiCJK_v2'), '|');
SELECT arrayStringConcat(tokens('hello😀world', 'asciiCJK_v2'), '|');
SELECT tokens('___', 'asciiCJK_v2');
SELECT length(tokens(repeat('中', 100), 'asciiCJK_v2'));

CREATE TABLE ascii_cjk_v2_index
(
    id UInt64,
    message String,
    INDEX text_idx(message) TYPE text(tokenizer = asciiCJK_v2, support_phrase_search = 1)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO ascii_cjk_v2_index VALUES (1, '错误503'), (2, 'taichi张三丰in the house'), (3, 'hello world');
ALTER TABLE ascii_cjk_v2_index MATERIALIZE INDEX text_idx;
SELECT groupArray(id) FROM ascii_cjk_v2_index WHERE hasPhrase(message, '张三', 'asciiCJK_v2');
DROP TABLE ascii_cjk_v2_index;
