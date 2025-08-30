-- Tags: no-fasttest, no-shared-merge-tree
-- Tag no-fasttest: requires Azure
-- Tag no-shared-merge-tree: does not support replication

DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32, b Int64) ENGINE = MergeTree() ORDER BY tuple(a, b)
SETTINGS storage_policy='azure_plain_rewritable_encrypted_03602';

INSERT INTO test (*) SELECT number, number from numbers_mt(1000);
SELECT count() FROM test;
