SET parallel_replicas_local_plan=1;


DROP TABLE IF EXISTS BadTable;
DROP TABLE IF EXISTS BadJoin;

CREATE TABLE IF NOT EXISTS BadTable (id_uint UInt128) ENGINE = MergeTree() ORDER BY id_uint;
INSERT INTO BadTable SELECT toUInt128(12);

CREATE TABLE IF NOT EXISTS BadJoin (id UUID, name LowCardinality(String)) ENGINE = MergeTree() ORDER BY (name);
INSERT INTO BadJoin SELECT '12a34567-8901-2345-6789-012345678901', '12';
SET enable_analyzer = 1;
-- TODO(@vdimir): NOT_FOUND_COLUMN_IN_BLOCK is a bug, should be fixed
-- This tests ensures that query does not crash at least

SELECT 1 FROM BadTable i LEFT JOIN BadJoin c ON i.id_uint = toUInt128(c.id) WHERE equals(i.id_uint, 12);

SELECT equals(i.id_uint, 12) FROM BadTable i LEFT JOIN BadJoin c ON i.id_uint = toUInt128(c.id) WHERE equals(i.id_uint, 12);
