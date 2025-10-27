-- This outputs the list of undocumented table functions. No new items in the list should appear.
-- Please help shorten this list down to zero elements.
SELECT name FROM system.table_functions WHERE length(description) < 10
AND name NOT IN (
    'cosn', 'oss', 'hdfs', 'hdfsCluster', 'hive', 'mysql', 'postgresql', 's3', 's3Cluster', 'sqlite', 'urlCluster', 'mergeTreeParts' -- these functions are not enabled in fast test
    , 'mongodb' -- will be removed when `use_legacy_mongodb_integration` setting will be purged will with the old implementation
) ORDER BY name;
