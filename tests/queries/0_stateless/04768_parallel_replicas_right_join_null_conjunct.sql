-- RIGHT JOIN whose ON clause carries a constant NULL conjunct segfaulted while the plan-based
-- parallel-replicas join built its output header (#121906): the left header can declare the
-- residual column by type only, and materializeBlockInplace dereferenced the missing column.

DROP TABLE IF EXISTS tb_l;
DROP TABLE IF EXISTS tb_r;

CREATE TABLE tb_l (b String) ENGINE = MergeTree ORDER BY b;
CREATE TABLE tb_r (b String) ENGINE = MergeTree ORDER BY b;

INSERT INTO tb_l VALUES ('a'), ('b');
INSERT INTO tb_r VALUES ('a'), ('c');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;

-- The conjunct never matches, so the RIGHT JOIN yields every right row; before the fix this died.
SELECT count() FROM tb_l AS l RIGHT JOIN tb_r AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8));
-- FULL JOIN walks the same left-materialization path and keeps unmatched rows from both sides.
SELECT count() FROM tb_l AS l FULL JOIN tb_r AS r ON l.b = r.b AND CAST(NULL AS Nullable(UInt8));
-- Control: a matching RIGHT JOIN still joins correctly.
SELECT count() FROM tb_l AS l RIGHT JOIN tb_r AS r ON l.b = r.b;

DROP TABLE tb_l;
DROP TABLE tb_r;
