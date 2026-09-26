SET analyzer_inline_views = 1;

CREATE TABLE security_barrier_04903 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO security_barrier_04903 VALUES (0), (1), (2);

-- `GROUP BY ALL` can hide rows without populating the regular AST field used
-- for explicit `GROUP BY`, so it must be a security barrier.
CREATE VIEW security_barrier_group_by_all_04903
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT number FROM security_barrier_04903 GROUP BY ALL;

SELECT count() FROM security_barrier_group_by_all_04903 WHERE number = 0;

DROP VIEW security_barrier_group_by_all_04903;
DROP TABLE security_barrier_04903;
