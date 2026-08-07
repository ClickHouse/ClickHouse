-- Redundant parentheses a user writes around a key (`PARTITION BY (a)`) are cosmetic, but since
-- https://github.com/ClickHouse/ClickHouse/pull/92340 the formatter preserves them, so a table
-- declared `PARTITION BY (a)` stopped matching an otherwise identical table declared
-- `PARTITION BY a` and `ATTACH PARTITION FROM` rejected the pair with
-- `Tables have different partition key`. Both tables here are created by the same server, so
-- this needs neither an upgrade nor a mixed-version cluster.

DROP TABLE IF EXISTS t_parens_src;
DROP TABLE IF EXISTS t_parens_dst;
DROP TABLE IF EXISTS t_parens_other;

CREATE TABLE t_parens_src (a UInt32, b UInt32, c UInt32)
ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (b) ORDER BY (b, c);

CREATE TABLE t_parens_dst (a UInt32, b UInt32, c UInt32)
ENGINE = MergeTree PARTITION BY a PRIMARY KEY b ORDER BY (b, c);

INSERT INTO t_parens_src VALUES (1, 1, 1);
ALTER TABLE t_parens_dst ATTACH PARTITION 1 FROM t_parens_src;
SELECT * FROM t_parens_dst ORDER BY ALL;

-- Only the comparison ignores the parentheses: what the user wrote is still stored and reported.
SELECT partition_key, primary_key, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_parens_src';

-- A genuinely different key must still be rejected.
CREATE TABLE t_parens_other (a UInt32, b UInt32, c UInt32)
ENGINE = MergeTree PARTITION BY (b) ORDER BY (b, c);
ALTER TABLE t_parens_other ATTACH PARTITION 1 FROM t_parens_src; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_parens_src;
DROP TABLE t_parens_dst;
DROP TABLE t_parens_other;
