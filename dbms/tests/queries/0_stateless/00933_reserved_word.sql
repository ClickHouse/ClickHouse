DROP TABLE IF EXISTS test.reserved_word_table;
CREATE TABLE test.reserved_word_table (`index` UInt8) ENGINE = MergeTree ORDER BY `index`;

DETACH TABLE test.reserved_word_table;
ATTACH TABLE test.reserved_word_table;

DROP TABLE test.reserved_word_table;
