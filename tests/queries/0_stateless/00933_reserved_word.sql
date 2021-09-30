-- Tags: no-parallel

DROP TABLE IF EXISTS reserved_word_table;
CREATE TABLE reserved_word_table (`index` UInt8) ENGINE = MergeTree ORDER BY `index`;

DETACH TABLE reserved_word_table;
ATTACH TABLE reserved_word_table;

DROP TABLE reserved_word_table;
