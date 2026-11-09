-- Tags: no-parallel
-- no-parallel: a row policy on `_table_function.mergeTreeProjection` is server-wide, so it filters (or, with
-- `throw_on_unmatched_row_policies`, refuses) every concurrent `mergeTreeProjection` read.

-- A row policy on the `mergeTreeProjection` table function applies to a read shipped as a serialized query plan.

DROP TABLE IF EXISTS t_tf_rp;
CREATE TABLE t_tf_rp (id UInt64, dept String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_tf_rp SELECT number, if(number % 2 = 0, 'eng', 'fin') FROM numbers(10);
ALTER TABLE t_tf_rp ADD PROJECTION p (SELECT id, dept ORDER BY dept);
ALTER TABLE t_tf_rp MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

DROP ROW POLICY IF EXISTS rp_tf_rp ON _table_function.mergeTreeProjection;
CREATE ROW POLICY rp_tf_rp ON _table_function.mergeTreeProjection FOR SELECT USING id < 3 TO CURRENT_USER;

SET serialize_query_plan = 1, prefer_localhost_replica = 0;

SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) ORDER BY id;
SELECT count() FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p'));
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) WHERE dept = 'fin' ORDER BY id;
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) PREWHERE dept = 'eng' ORDER BY id
    SETTINGS use_query_condition_cache = 1;
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) ORDER BY id DESC LIMIT 2;
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p'))
    PREWHERE throwIf(id >= 3, 'row policy leak') = 0 ORDER BY id;
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p'))
    WHERE throwIf(id >= 3, 'row policy leak') = 0 ORDER BY id;

ALTER ROW POLICY rp_tf_rp ON _table_function.mergeTreeProjection USING id IN (SELECT number + 7 FROM numbers(3));
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) ORDER BY id;

DROP ROW POLICY rp_tf_rp ON _table_function.mergeTreeProjection;
SELECT count() FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p'));
SELECT id FROM cluster('test_shard_localhost', mergeTreeProjection(currentDatabase(), 't_tf_rp', 'p')) PREWHERE dept = 'eng' ORDER BY id
    SETTINGS use_query_condition_cache = 1;

DROP TABLE t_tf_rp;
