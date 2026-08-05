-- Tags: stateful
DROP TABLE IF EXISTS hits_none;
CREATE TABLE hits_none (Title String CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO hits_none SELECT Title FROM test.hits SETTINGS enable_filesystem_cache_on_write_operations=0, max_insert_threads=16;

SET min_bytes_to_use_mmap_io = 1;
SELECT sum(length(Title)) FROM hits_none;

DROP TABLE hits_none;
