DROP TABLE IF EXISTS bf_tokenbf_map_test;
DROP TABLE IF EXISTS bf_ngram_map_test;

CREATE TABLE bf_tokenbf_map_test
(
    row_id UInt32,
    map Map(String, String),
    INDEX map_tokenbf map TYPE tokenbf_v1(256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 2;

CREATE TABLE bf_ngram_map_test
(
    row_id UInt32,
    map Map(String, String),
    map_fixed Map(FixedString(2), String),
    INDEX map_ngram map TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_ngram map_fixed TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 2;

INSERT INTO bf_tokenbf_map_test VALUES (1, {'K1':'V1'}, {'K1':'V1'}), (2, {'K2':'V2'}, {'K2':'V2'});
INSERT INTO bf_ngram_map_test VALUES (1, {'K1':'V1'}, {'K1':'V1'}), (2, {'K2':'V2'}, {'K2':'V2'});

SELECT * FROM bf_tokenbf_map_test WHERE map['K1']='V1';
SELECT * FROM bf_ngram_map_test WHERE map['K2']='V2';

SELECT * FROM bf_tokenbf_map_test WHERE map_fixed['K1']='V1';
SELECT * FROM bf_ngram_map_test WHERE map_fixed['K2']='V2';

DROP TABLE bf_tokenbf_map_test;
DROP TABLE bf_ngram_map_test;
