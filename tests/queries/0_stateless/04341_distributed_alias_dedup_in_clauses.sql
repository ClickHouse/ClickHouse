-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/107507
-- Two ALIAS columns expanding to the same expression, referenced by alias in
-- ORDER BY / GROUP BY / HAVING on a Distributed table, must not throw
-- MULTIPLE_EXPRESSIONS_FOR_ALIAS (introduced while fixing #105689 in #106404).

DROP TABLE IF EXISTS local_alias_dedup;
DROP TABLE IF EXISTS dist_alias_dedup;

CREATE TABLE local_alias_dedup
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

CREATE TABLE dist_alias_dedup
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_alias_dedup);

INSERT INTO local_alias_dedup VALUES ('2024-01-01 00:00:00', 7), ('2024-01-02 00:00:00', 9), ('2024-01-03 00:00:00', 11);

-- remote() table function path (single shard).
SELECT a1, a2 FROM remote('127.0.0.1', currentDatabase(), local_alias_dedup) ORDER BY a2;
SELECT a1, a2 FROM remote('127.0.0.1', currentDatabase(), local_alias_dedup) GROUP BY a1, a2 ORDER BY a1;
SELECT a1, a2, count() AS c FROM remote('127.0.0.1', currentDatabase(), local_alias_dedup) GROUP BY a1, a2 HAVING a2 = '7' ORDER BY a1;

-- Distributed engine path (single shard) — same expected output.
SELECT a1, a2 FROM dist_alias_dedup ORDER BY a2;
SELECT a1, a2 FROM dist_alias_dedup GROUP BY a1, a2 ORDER BY a1;
SELECT a1, a2, count() AS c FROM dist_alias_dedup GROUP BY a1, a2 HAVING a2 = '7' ORDER BY a1;

DROP TABLE dist_alias_dedup;
DROP TABLE local_alias_dedup;
