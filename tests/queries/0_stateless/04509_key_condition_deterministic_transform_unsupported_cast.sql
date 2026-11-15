-- The deterministic key-transform path pushes an equality constant through key-side functions by
-- casting it to the transform's input type first. For some type pairs that cast is not implemented
-- (for example, `IPv6` into `FixedString(16)`), and building it throws. Such candidates must be
-- skipped so the query still runs without the index shortcut. Each check prints the predicate, the
-- count with index analysis, the count of a full scan and the marks selected by index analysis.

CREATE TABLE test_unsupported_cast (s FixedString(16)) ENGINE = MergeTree ORDER BY reverse(s);
INSERT INTO test_unsupported_cast VALUES ('0123456789abcdef');

SELECT 's = toIPv6(\'7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995\')', (SELECT count() FROM test_unsupported_cast WHERE s = toIPv6('7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995')),
    (SELECT count() FROM test_unsupported_cast WHERE s = toIPv6('7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0),
    (SELECT sum(marks) FROM (EXPLAIN ESTIMATE SELECT 1 FROM test_unsupported_cast WHERE s = toIPv6('7afe:b9d4:e754:4e78:8783:37f5:b2ea:9995')))
FORMAT TSVRaw;
-- The IPv6 value whose 16 bytes spell '0123456789abcdef'.
SELECT 's = toIPv6(\'3031:3233:3435:3637:3839:6162:6364:6566\')', (SELECT count() FROM test_unsupported_cast WHERE s = toIPv6('3031:3233:3435:3637:3839:6162:6364:6566')),
    (SELECT count() FROM test_unsupported_cast WHERE s = toIPv6('3031:3233:3435:3637:3839:6162:6364:6566') SETTINGS use_primary_key = 0, use_partition_pruning = 0, use_skip_indexes = 0),
    (SELECT sum(marks) FROM (EXPLAIN ESTIMATE SELECT 1 FROM test_unsupported_cast WHERE s = toIPv6('3031:3233:3435:3637:3839:6162:6364:6566')))
FORMAT TSVRaw;
