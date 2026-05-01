-- Tags: no-fasttest
-- Test: exercises ColumnIPv6 hashing branch in FunctionsStringHashFixedString.cpp:331-337
-- with DIFFERENT per-row IPv6 values to verify correct per-row indexing &data[i].
-- The PR's own test (02998_ipv6_hashing.sql) materializes a SINGLE IPv6 value 10 times,
-- which only catches the original out-of-bounds bug (fixed: &data[i*length] -> &data[i])
-- but would NOT catch a regression like &data[0] (always first row) since all values are equal.
-- This test ensures distinct IPv6 inputs produce distinct hashes.

DROP TABLE IF EXISTS t_ipv6_hash;
CREATE TABLE t_ipv6_hash (id UInt32, addr IPv6) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ipv6_hash VALUES (1, '::1'), (2, '::2'), (3, 'fe80::1'), (4, '2001:db8::1');

-- Per-row hashes must reflect per-row inputs (mutation guard: if &data[i] -> &data[0],
-- all rows would print the hash of '::1' four times).
SELECT id, hex(SHA256(addr)) FROM t_ipv6_hash ORDER BY id;

-- Cardinality check: 4 distinct inputs must yield 4 distinct hashes.
SELECT count(DISTINCT hex(SHA256(addr))) FROM t_ipv6_hash;

DROP TABLE t_ipv6_hash;
