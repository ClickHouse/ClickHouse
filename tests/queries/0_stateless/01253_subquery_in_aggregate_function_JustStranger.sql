DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_sharded;

set allow_deprecated_syntax_for_merge_tree=1;
create table
  test_table_sharded(
    date Date,
    text String,
    hash UInt64
  )
engine=MergeTree(date, (hash, date), 8192);

create table test_table as test_table_sharded
engine=Distributed(test_cluster_two_shards, currentDatabase(), test_table_sharded, hash);

SET distributed_product_mode = 'local';
SET insert_distributed_sync = 1;

INSERT INTO test_table VALUES ('2020-04-20', 'Hello', 123);

SELECT
  text,
  uniqExactIf(hash, hash IN (
    SELECT DISTINCT
      hash
    FROM test_table AS t1
  )) as counter
FROM test_table AS t2
GROUP BY text
ORDER BY counter, text;

DROP TABLE test_table;
DROP TABLE test_table_sharded;
