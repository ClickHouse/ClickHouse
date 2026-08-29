-- `has(<constant Array(Enum)>, <String column>)` compares by the name of the enum value, so a
-- `bloom_filter` index on the searched column has to hash the names of the enum values.
-- Hashing their numeric payload would prune the granules that actually match.

DROP TABLE IF EXISTS t_has_enum_bf;

CREATE TABLE t_has_enum_bf
(
    id UInt64,
    s String,
    f FixedString(2),
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1,
    INDEX idx_f f TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_has_enum_bf VALUES (1, 'a', 'a'), (2, 'b', 'b'), (3, '1', '1');

SELECT id FROM t_has_enum_bf WHERE has(CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), s) ORDER BY id;
SELECT id FROM t_has_enum_bf WHERE has(CAST(['a', 'b'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), s) ORDER BY id;
SELECT id FROM t_has_enum_bf WHERE has(CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), f) ORDER BY id;

-- The index is used and prunes the granules that do not have the name of the enum value.
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_has_enum_bf WHERE has(CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), s)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

-- The same without the index.
SELECT id FROM t_has_enum_bf WHERE has(CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), s) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_has_enum_bf WHERE has(CAST(['a'], 'Array(Enum8(\'a\' = 1, \'b\' = 2))'), f) ORDER BY id SETTINGS use_skip_indexes = 0;

DROP TABLE t_has_enum_bf;
