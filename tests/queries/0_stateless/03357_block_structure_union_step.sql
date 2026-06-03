CREATE TABLE pk_block_union (d Date DEFAULT '2000-01-01', x DateTime, y UInt64, z UInt64) ENGINE = MergeTree() PARTITION BY d ORDER BY (toStartOfMinute(x), y, z);

INSERT INTO pk_block_union (x, y, z) VALUES (1, 11, 1235), (2, 11, 4395), (3, 22, 3545), (4, 22, 6984), (5, 33, 4596), (61, 11, 4563), (62, 11, 4578), (63, 11, 3572), (64, 22, 5786), (65, 22, 5786), (66, 22, 2791), (67, 22, 2791), (121, 33, 2791), (122, 33, 2791), (123, 33, 1235), (124, 44, 4935), (125, 44, 4578), (126, 55, 5786), (127, 55, 2791), (128, 55, 1235);

SELECT cityHash64(1, (x = 3) AND (y = 44), '0', 1, 1, 1, 1, 1, 1, 1, toLowCardinality(1), 1, 1, 1, 1, 1, toNullable(toUInt256(1)), 1, 1, 1, toUInt128(1), 1, toLowCardinality(toUInt128(1)), 1, 1), cityHash64('0', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, toUInt128(1), 1, 1, toLowCardinality(1), 1, 1, toLowCardinality(1), toLowCardinality(1), 1, 1, 1, 1, 1), * FROM pk_block_union WHERE (x = 3) AND (y = 44) ORDER BY ALL DESC;
