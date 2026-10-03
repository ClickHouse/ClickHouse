-- The first occurrence of a key in a disk description wins when the disk is created, while the access
-- check of `mergeTreeParts` runs before that on the description as written. So every key that decides
-- the source or its location may be given only once: otherwise the check could look at one value while
-- the disk uses another.

SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = local, type = s3, endpoint = 'http://localhost:11111/test/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = object_storage, object_storage_type = local, object_storage_type = s3, endpoint = 'http://localhost:11111/test/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = s3, endpoint = 'http://localhost:11111/test/a/', endpoint = 'http://localhost:11111/test/b/'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = azure_blob_storage, storage_account_url = 'http://localhost:10000/devstoreaccount1', storage_account_url = 'http://localhost:10000/other', container_name = 'cont'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = azure_blob_storage, storage_account_url = 'http://localhost:10000/devstoreaccount1', container_name = 'a', container_name = 'b'),
    table_settings(index_granularity_bytes = 10485760)); -- { serverError BAD_ARGUMENTS }

-- Only `0` and `1` are format versions.
SELECT * FROM mergeTreeParts(structure('x UInt8'), parts(),
    disk(type = local, path = '/'),
    table_settings(index_granularity_bytes = 10485760, format_version = 2)); -- { serverError BAD_ARGUMENTS }
