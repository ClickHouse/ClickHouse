-- Tags: no-fasttest
-- Tag no-fasttest: needs minio (object storage)

-- A `GLOBAL IN` subquery over `_path` / `_file` of an object-storage path without globs used to throw
-- "Not-ready Set is passed as the second argument for function 'globalIn'".
-- `ReadFromObjectStorageStep::applyFilters` leaves the sets of `globalIn` / `globalNotIn` unbuilt so
-- that `ReadFromRemote` can attach an external table to them first; plan optimization then moves the
-- subquery plan of such a set into `CreatingSetsStep`, and the key pruning in
-- `StorageObjectStorageSource::createFileIterator`, which runs while the pipeline is built, executed
-- the filter with a set that is only created when the pipeline runs.

INSERT INTO FUNCTION s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase()), format = CSV, structure = 'x UInt64')
SELECT 1
SETTINGS s3_truncate_on_insert = 1;

SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL NOT IN (SELECT 'no such path');

SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE _file GLOBAL IN (SELECT concat('04669_path_filter_global_in_', currentDatabase()));

SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE globalNotIn(_file, (SELECT 'no such file'));

-- A nonexistent key that the predicate excludes must return no rows, not throw FILE_DOESNT_EXIST:
-- the filter has to be applied before the key's metadata is probed. With a not-ready set the pruning
-- cannot happen while the pipeline is built, so `KeysIterator` applies the filter lazily, when the
-- pipeline runs and the set is ready.
SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_no_such_file_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_no_such_file_', currentDatabase()), format = CSV, structure = 'x UInt64')
WHERE _file GLOBAL NOT IN (SELECT concat('04669_path_filter_global_in_no_such_file_', currentDatabase()));

-- A pure brace expansion also uses `KeysIterator`, so it must defer a not-ready predicate
-- and filter a nonexistent key before probing its metadata.
SELECT * FROM s3(s3_conn, filename = concat('{04669_path_filter_global_in_no_such_file_', currentDatabase(), ',04669_path_filter_global_in_', currentDatabase(), '}'), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path');

-- Hive partition columns are inferred from the sample path string alone, so an explicitly specified
-- key must keep them even when the object does not exist and the predicate excludes it: the cluster
-- variant samples the path during analysis, and a missing key must not erase the sample. Losing the
-- sample made the query throw UNKNOWN_IDENTIFIER for `date` instead of returning 0 rows.
SELECT x, date FROM s3Cluster('test_shard_localhost', s3_conn, filename = concat('04669_path_filter_global_in_hive_', currentDatabase(), '/date=2026-08-12/missing.csv'), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path')
SETTINGS use_hive_partitioning = 1;

-- A pure brace expansion needs the same local sample-path fallback for Hive partitioning.
SELECT x, date FROM s3Cluster('test_shard_localhost', s3_conn, filename = concat('{04669_path_filter_global_in_hive_', currentDatabase(), '/date=2026-08-12/missing.csv,04669_path_filter_global_in_hive_', currentDatabase(), '/date=2026-08-13/missing.csv}'), format = CSV, structure = 'x UInt64')
WHERE _path GLOBAL IN (SELECT 'no such path')
SETTINGS use_hive_partitioning = 1;

-- A glob applies the same filter lazily, while listing objects; it must keep working too.
SELECT * FROM s3(s3_conn, filename = concat('04669_path_filter_global_in_', currentDatabase(), '*'), format = CSV, structure = 'x UInt64')
WHERE _file GLOBAL IN (SELECT concat('04669_path_filter_global_in_', currentDatabase()));
