SET analyzer_inline_views = 1;

CREATE TABLE security_barrier_04903 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO security_barrier_04903 VALUES (0), (1), (2);

-- Both shapes can hide rows without populating the regular AST fields used for
-- explicit `GROUP BY` and `LIMIT`. They must therefore be security barriers.
CREATE VIEW security_barrier_group_by_all_04903
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT number FROM security_barrier_04903 GROUP BY ALL;

CREATE VIEW security_barrier_settings_04903
DEFINER = CURRENT_USER SQL SECURITY DEFINER
AS SELECT number FROM security_barrier_04903 SETTINGS limit = 1;

SELECT count() FROM security_barrier_group_by_all_04903 WHERE number = 0;

-- If the view is inlined, the predicate is evaluated before the query-local
-- limit and `throwIf` observes the row that the view hides.
SELECT number FROM security_barrier_settings_04903 WHERE throwIf(number = 2, 'LEAKED');

DROP VIEW security_barrier_group_by_all_04903;
DROP VIEW security_barrier_settings_04903;
DROP TABLE security_barrier_04903;
