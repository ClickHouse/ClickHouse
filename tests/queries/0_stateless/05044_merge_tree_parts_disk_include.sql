-- Tags: no-fasttest

-- `mergeTreeParts` checks the `READ` grant for the source that matches the disk `type` written in the
-- query, and it does so before the disk is created. `include` would let the description resolve to a
-- different backend afterwards - a non-S3 `type` could be checked while an S3 disk gets created - so
-- `include` is rejected while parsing the arguments, even when `dynamic_disk_allow_include` is on.

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/', include = 'locations'),
    table_settings(index_granularity_bytes = 10485760))
SETTINGS dynamic_disk_allow_include = 1; -- { serverError BAD_ARGUMENTS }

SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(type = local, path = '/', include = 'locations'),
    table_settings(index_granularity_bytes = 10485760))
SETTINGS dynamic_disk_allow_include = 0; -- { serverError BAD_ARGUMENTS }

-- The rejection does not depend on the position of `include` in the description, nor on the `type` at all.
SELECT * FROM mergeTreeParts(
    structure('x UInt8'),
    parts(),
    disk(include = 'locations', type = s3, endpoint = 'http://localhost:11111/test/', access_key_id = 'a', secret_access_key = 'b'),
    table_settings(index_granularity_bytes = 10485760))
SETTINGS dynamic_disk_allow_include = 1; -- { serverError BAD_ARGUMENTS }
