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

-- Direct expression written next to an ALIAS column with the same alias: both projection items inline
-- to the same expression and carry alias 'a2', but they must remain two distinct shard outputs. The
-- sibling rewrite must touch the ORDER BY key only, not the projection items themselves (otherwise the
-- two outputs collapse into one -> NUMBER_OF_COLUMNS_DOESNT_MATCH).
SELECT toString(x) AS a2, a2 FROM remote('127.0.0.1', currentDatabase(), local_alias_dedup) ORDER BY a2;

-- Alias referenced inside a projection expression (not as a top-level projection item) must be rewritten.
SELECT a1, concat(a2, a2) AS cc FROM remote('127.0.0.1', currentDatabase(), local_alias_dedup) ORDER BY a2;

DROP TABLE dist_alias_dedup;
DROP TABLE local_alias_dedup;
