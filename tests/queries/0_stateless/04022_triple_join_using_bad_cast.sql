-- https://github.com/ClickHouse/ClickHouse/issues/93012
CREATE TABLE t0 (c0 Int) ENGINE = Memory;
INSERT INTO t0 VALUES (1);
SELECT abs(1) c0 FROM t0 JOIN t0 ty ON t0.c0 = ty.c0 JOIN t0 tx USING (c0) SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;
