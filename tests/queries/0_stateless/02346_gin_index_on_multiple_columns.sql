-- Tests that GIN indexes can be created on > 1 column

SET allow_experimental_full_text_index = 1;

DROP TABLE IF EXISTS multi_col_ivt;

CREATE TABLE tab (
    v0 String,
    v1 String,
    INDEX idx (v0, v1) TYPE gin GRANULARITY 1)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1,
         min_bytes_for_full_part_storage = 0; -- GIN indexes currently don't work with packed parts

INSERT INTO tab VALUES('0,0', '0,1')('2,2','2,3');

SELECT 'Query column at granularity boundary';
SELECT * FROM tab WHERE hasToken(v1, '1');

DROP TABLE tab;
