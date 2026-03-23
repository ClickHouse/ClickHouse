-- This outputs the list of undocumented table functions.
-- No new items in the list should appear. Please help shorten this list down to zero elements.
SELECT name FROM system.table_functions WHERE length(description) < 10
AND name NOT IN (
    -- these table functions are not enabled in fast test
    'cosn', 'oss', 'hdfs', 'hdfsCluster', 'hive', 'mysql', 'postgresql', 's3', 's3Cluster', 'sqlite', 'urlCluster', 'mergeTreeParts'
) ORDER BY name;
