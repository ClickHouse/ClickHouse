-- { echo }

-- `arrayIndex` unwraps the null map before `has` and `indexOf` dispatch to the String specialization.
-- The padded `FixedString` therefore compares as raw bytes, and the bloom-filter index must retain
-- the row whose nullable String has matching padding.

CREATE TABLE t (id UInt64, v Array(Nullable(String)), INDEX idx v TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t VALUES (0, ['V0']), (1, ['V0\0']), (2, ['X']);

SELECT id FROM t WHERE has(v, toFixedString('V0', 3)) ORDER BY id;
SELECT id FROM t WHERE indexOf(v, toFixedString('V0', 3)) != 0 ORDER BY id;

DROP TABLE t;
