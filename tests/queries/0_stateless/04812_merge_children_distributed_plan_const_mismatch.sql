-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- Tag no-ordinary-database: Sometimes cannot lock file most likely due to concurrent or adjacent tests, but we don't care how it works in Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

-- Reproducer for a logical error (Block structure mismatch) found by the AST fuzzer:
-- with `make_distributed_plan`, optimizing the child plans of a `Merge` table ran
-- `materializeConstantsForSetOperationBranches` on them, so the `Buffer` child (whose plan
-- contains a `Union` of the destination table and the buffers) exposed materialized constants
-- while the `EmbeddedRocksDB` child kept them const, and uniting the children failed.

DROP TABLE IF EXISTS 04812_merge_dp_rocksdb;
DROP TABLE IF EXISTS 04812_merge_dp_buffer;

CREATE TABLE 04812_merge_dp_rocksdb (k UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (k);
CREATE TABLE 04812_merge_dp_buffer ENGINE = Buffer(currentDatabase(), '04812_merge_dp_rocksdb', 1, 1, 10, 10000, 1000000, 10000000, 100000000);

INSERT INTO 04812_merge_dp_rocksdb VALUES (1), (2);

SET make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT DISTINCT 42 FROM merge(currentDatabase(), '^04812_merge_dp_') QUALIFY materialize(42);

-- The same shape with the narrowing path (`max_threads = 1` narrows the united pipes).
SELECT DISTINCT 42 FROM merge(currentDatabase(), '^04812_merge_dp_') QUALIFY materialize(42) SETTINGS max_threads = 1;

DROP TABLE 04812_merge_dp_buffer;
DROP TABLE 04812_merge_dp_rocksdb;
