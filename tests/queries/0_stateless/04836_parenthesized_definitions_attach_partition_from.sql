-- Tags: no-random-merge-tree-settings
-- Tag no-random-merge-tree-settings: the test shows the definition of a table, and the randomized
-- settings would be printed with it.

-- Whether the user wrote redundant parentheses around a definition expression is not a property of
-- the table: `PARTITION BY (a)` and `PARTITION BY a` are the same key. `ATTACH PARTITION FROM`
-- compares the definitions of the two tables as text, and that text must not see the parentheses.

DROP TABLE IF EXISTS t_parens_src;
DROP TABLE IF EXISTS t_parens_dst;

CREATE TABLE t_parens_src (x UInt64, y UInt64, z UInt64,
    INDEX ix (y * z) TYPE minmax,
    PROJECTION p (SELECT (y) ORDER BY z))
ENGINE = MergeTree PARTITION BY (x) PRIMARY KEY (y) ORDER BY (y, z);

CREATE TABLE t_parens_dst (x UInt64, y UInt64, z UInt64,
    INDEX ix y * z TYPE minmax,
    PROJECTION p (SELECT y ORDER BY z))
ENGINE = MergeTree PARTITION BY x PRIMARY KEY y ORDER BY (y, z);

INSERT INTO t_parens_src VALUES (1, 2, 3);
ALTER TABLE t_parens_dst ATTACH PARTITION 1 FROM t_parens_src;
SELECT * FROM t_parens_dst;

-- The parentheses do not have to be at the top level of the expression.
DROP TABLE IF EXISTS t_nested_parens_src;
DROP TABLE IF EXISTS t_nested_parens_dst;

CREATE TABLE t_nested_parens_src (x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY (x + (1)) ORDER BY y;
CREATE TABLE t_nested_parens_dst (x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY x + 1 ORDER BY y;

INSERT INTO t_nested_parens_src VALUES (1, 2);
ALTER TABLE t_nested_parens_dst ATTACH PARTITION 2 FROM t_nested_parens_src;
SELECT * FROM t_nested_parens_dst;

-- Definitions that differ in more than the parentheses are still rejected.
DROP TABLE IF EXISTS t_other_key;
CREATE TABLE t_other_key (x UInt64, y UInt64) ENGINE = MergeTree PARTITION BY x + 2 ORDER BY y;
ALTER TABLE t_other_key ATTACH PARTITION 2 FROM t_nested_parens_src; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS t_other_index;
CREATE TABLE t_other_index (x UInt64, y UInt64, z UInt64,
    INDEX ix y + z TYPE minmax,
    PROJECTION p (SELECT y ORDER BY z))
ENGINE = MergeTree PARTITION BY x PRIMARY KEY y ORDER BY (y, z);
ALTER TABLE t_other_index ATTACH PARTITION 1 FROM t_parens_src; -- { serverError BAD_ARGUMENTS }

-- Restating a `TTL` with parentheses added is not a change, so it schedules no mutation.
DROP TABLE IF EXISTS t_parens_ttl;
CREATE TABLE t_parens_ttl (x UInt64, d Date) ENGINE = MergeTree ORDER BY x TTL d + INTERVAL 10 YEAR;
ALTER TABLE t_parens_ttl MODIFY TTL (d + INTERVAL 10 YEAR);
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_parens_ttl';

-- The stored definition still shows the parentheses exactly as they were written.
SHOW CREATE TABLE t_nested_parens_src;
SHOW CREATE TABLE t_parens_ttl;
