-- insert_deduplication_token must work correctly if the inserting is performed in multiple threads.

DROP TABLE IF EXISTS tbl;

CREATE TABLE tbl(a UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/{database}/test_02875/replicated_mergetree_dedup_token_mt', 'r1') ORDER BY tuple() SETTINGS non_replicated_deduplication_window=1000;

INSERT INTO tbl SETTINGS max_insert_threads = 8, insert_deduplication_token = 'dedup' SELECT * FROM numbers_mt(10000) SETTINGS max_block_size=100;
SELECT min(a), max(a), count() FROM tbl;

INSERT INTO tbl SETTINGS max_insert_threads = 8, insert_deduplication_token = 'dedup' SELECT * FROM numbers_mt(10000) SETTINGS max_block_size=100;
SELECT min(a), max(a), count() FROM tbl;

INSERT INTO tbl SETTINGS max_insert_threads = 8, insert_deduplication_token = 'dedup2' SELECT * FROM numbers_mt(10000) SETTINGS max_block_size=100;
SELECT min(a), max(a), count() FROM tbl;

DROP TABLE tbl;
