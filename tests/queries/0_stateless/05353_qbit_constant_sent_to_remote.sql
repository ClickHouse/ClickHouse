-- A QBit constant has to reach another server as a literal that casts back to the bit-identical QBit value.
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_qbit_dist;
DROP TABLE IF EXISTS t_qbit;
CREATE TABLE t_qbit (id UInt32, q QBit(Float32, 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qbit VALUES (1, [1, 2]), (2, [3, 4]), (3, [-0., inf]), (4, [0., inf]), (6, [nan, 1]);
-- NaNs that differ from the literal `nan` (0x7FC00000) only in the sign bit (5: 0xFFC00000) or only in the payload (7: 0x7FC00001).
INSERT INTO t_qbit SELECT 5, CAST([reinterpretAsFloat32(toUInt32(4290772992)), 1], 'QBit(Float32, 2)');
INSERT INTO t_qbit SELECT 7, CAST([reinterpretAsFloat32(toUInt32(2143289345)), 1], 'QBit(Float32, 2)');
CREATE TABLE t_qbit_dist AS t_qbit ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_qbit);

SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q = CAST([1, 2], 'QBit(Float32, 2)');
SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q = CAST([-0., inf], 'QBit(Float32, 2)');
SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q = CAST([reinterpretAsFloat32(toUInt32(4290772992)), 1], 'QBit(Float32, 2)');
SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q = CAST([reinterpretAsFloat32(toUInt32(2143289345)), 1], 'QBit(Float32, 2)');
SELECT id FROM t_qbit_dist WHERE q = CAST([3, 4], 'QBit(Float32, 2)');
SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q IN (CAST([1, 2], 'QBit(Float32, 2)'), CAST([3, 4], 'QBit(Float32, 2)')) ORDER BY id;
SELECT id FROM remote('127.0.0.2', currentDatabase(), t_qbit) WHERE q = CAST([1, 2], 'QBit(Float32, 2)') OR q = CAST([3, 4], 'QBit(Float32, 2)') OR q = CAST([5, 6], 'QBit(Float32, 2)') ORDER BY id;
SELECT id FROM (SELECT id, q FROM remote('127.0.0.2', currentDatabase(), t_qbit)) WHERE q = CAST([3, 4], 'QBit(Float32, 2)');
SELECT id FROM t_qbit WHERE q = CAST([1, 2], 'QBit(Float32, 2)') SETTINGS enable_parallel_replicas = 2, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 0;

-- optimize_const_name_size = -1 keeps the larger constants in the query text instead of sending them as scalars.
SELECT CAST([1, -2], 'QBit(Int8, 2)'), CAST([1.5, 2], 'QBit(BFloat16, 2)'), CAST([0.1, 2], 'QBit(Float64, 2)'), CAST(range(16), 'QBit(Float32, 16, 8)') FROM remote('127.0.0.2', system.one) SETTINGS optimize_const_name_size = -1;
SELECT [CAST([1, 2], 'QBit(Float32, 2)')], (CAST([1, 2], 'QBit(Float32, 2)'), 1), map('a', CAST([1, 2], 'QBit(Float32, 2)')),
    CAST([1, 2], 'Nullable(QBit(Float32, 2))'), CAST(NULL, 'Nullable(QBit(Float32, 2))') FROM remote('127.0.0.2', system.one);
SELECT CAST(CAST([1, 2], 'QBit(Float32, 2)'), 'Variant(QBit(Float32, 2), String)'), CAST(CAST([1, 2], 'QBit(Float32, 2)'), 'Dynamic') FROM remote('127.0.0.2', system.one);
-- `materialize` makes the other server report the member type of the value it received.
SELECT variantType(materialize(CAST(CAST([1, 2], 'QBit(Float32, 2)'), 'Variant(QBit(Float32, 2), String)'))), dynamicType(materialize(CAST(CAST([1, 2], 'QBit(Float32, 2)'), 'Dynamic'))),
    dynamicType(materialize(CAST(CAST([1, 2], 'QBit(Float32, 2)'), 'Dynamic(max_types = 0)'))) FROM remote('127.0.0.2', system.one);

DROP TABLE t_qbit_dist;
DROP TABLE t_qbit;
