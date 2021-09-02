CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.bf_tokenbf_map_test;
DROP TABLE IF EXISTS test.bf_ngram_map_test;

CREATE TABLE test.bf_tokenbf_map_test (row_id UInt32, map Map(String, String), INDEX map_tokenbf map TYPE tokenbf_v1(256,2,0) GRANULARITY 1) Engine=MergeTree() ORDER BY row_id settings index_granularity = 2;
CREATE TABLE test.bf_ngram_map_test (row_id UInt32, map Map(String, String), INDEX map_tokenbf map TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1) Engine=MergeTree() ORDER BY row_id settings index_granularity = 2;

INSERT INTO test.bf_tokenbf_map_test VALUES (1, {'K1':'V1'}),(2,{'K2':'V2'}),(3,{'K3':'V3'}),(4,{'K4':'V4'});
INSERT INTO test.bf_ngram_map_test VALUES (1, {'K1':'V1'}),(2,{'K2':'V2'}),(3,{'K3':'V3'}),(4,{'K4':'V4'});

SELECT * FROM test.bf_tokenbf_map_test WHERE map['K3']='V3';
SELECT * FROM test.bf_tokenbf_map_test WHERE map['K2']='V2';

DROP TABLE test.bf_tokenbf_map_test; 
DROP TABLE test.bf_ngram_map_test;