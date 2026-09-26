-- Tags: no-fasttest

SET enable_analyzer = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_04317_data.tsv', 'TSV', 'x UInt8')
SELECT number FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

-- recursive_with on the query feeding an IStorageCluster table function used to
-- segfault inside AddDefaultDatabaseVisitor (null WITH clause). See #105370.
WITH RECURSIVE r AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM r WHERE n < 3
)
SELECT count() FROM fileCluster('test_cluster_one_shard_two_replicas', currentDatabase() || '_04317_data.tsv', 'TSV', 'x UInt8');
