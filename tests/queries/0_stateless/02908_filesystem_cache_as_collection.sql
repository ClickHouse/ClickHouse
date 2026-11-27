-- Tags: no-fasttest, no-replicated-database

CREATE NAMED COLLECTION IF NOT EXISTS cache_collection_sql AS path = 'collection_sql', max_size = '1Mi';
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a SETTINGS disk = disk(type = cache, disk = 'local_disk', name = 'cache_with_sql_collection', cache_name='cache_collection_sql', load_metadata_asynchronously = 0);
select path from system.filesystem_cache_settings where cache_name = 'cache_with_sql_collection';
CREATE TABLE test2 (a Int32, b String)
ENGINE = MergeTree() ORDER BY a SETTINGS disk = disk(type = cache, disk = 'local_disk', name = 'cache_with_collection', cache_name='cache_collection', load_metadata_asynchronously = 0);
select path from system.filesystem_cache_settings where cache_name = 'cache_with_collection';
