SET distributed_optimize_skip_select_on_unused_shards = 1;

DROP TABLE IF EXISTS test.mergetree;
DROP TABLE IF EXISTS test.distributed;

CREATE TABLE test.mergetree (a Int64, b Int64) ENGINE = MergeTree ORDER BY (a, b);
CREATE TABLE test.distributed AS test.mergetree ENGINE = Distributed(test_unavailable_shard, test, mergetree, jumpConsistentHash(a+b, 2));

INSERT INTO test.mergetree VALUES (0, 0);
INSERT INTO test.mergetree VALUES (1, 0);
INSERT INTO test.mergetree VALUES (0, 1);
INSERT INTO test.mergetree VALUES (1, 1);

/* without setting, quering of the second shard will fail because it isn't available */

SELECT jumpConsistentHash(a+b, 2) FROM test.distributed 
WHERE (a+b > 0 AND a = 0 AND b = 0) OR (a IN (0, 1) AND b IN (0, 1)) OR ((a = 1 OR a = 1) AND b = 2);
