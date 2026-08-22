-- Tags: no-old-analyzer
-- The bug and its fix are in the Analyzer's Planner (`collectSets`), and the old analyzer cannot
-- execute these mutations at all (the ALIAS -> ALIAS -> IN chain in a mutation predicate throws
-- UNKNOWN_IDENTIFIER). Background mutations run with server-default settings, so a session-level
-- `SET enable_analyzer = 1` is not enough and the test must be skipped in old-analyzer runs.

-- ALTER validation and mutations must not abort with LOGICAL_ERROR "No set is registered
-- for key" when the table has an ALIAS column referencing another ALIAS column that uses an
-- IN expression. Expanding such ALIAS expressions runs PlannerActionsVisitor, which resolves
-- IN via the prepared sets, so the sets must be registered (collectSets) before the columns
-- are collected. Covers the DROP COLUMN validator and the DELETE/UPDATE mutation paths.

SET mutations_sync = 2;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_alias_in_set;

CREATE TABLE t_alias_in_set
(
    id UInt64,
    category String,
    extra UInt32 DEFAULT 0,
    is_special UInt8 ALIAS category IN ('electronics', 'clothing', 'food'),
    label String ALIAS if(is_special, 'YES', 'NO')
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO t_alias_in_set (id, category) VALUES (1, 'electronics'), (2, 'other'), (3, 'food');

-- DROP COLUMN validation expands ALIAS expressions (AlterCommands::validate).
ALTER TABLE t_alias_in_set DROP COLUMN extra;
SELECT 'after drop', id, is_special, label FROM t_alias_in_set ORDER BY id;

-- Correlated scalar subquery referencing the ALIAS chain (found by the AST fuzzer on this PR).
-- collectSets does not descend into subqueries, so the ALIAS expansion must register
-- the sets of the ALIAS expression itself.
SELECT 'subquery select', id, 'YES' IS DISTINCT FROM (SELECT label) FROM t_alias_in_set ORDER BY id;
SELECT 'subquery where', count() FROM t_alias_in_set WHERE 'YES' IS DISTINCT FROM (SELECT label);
-- In a mutation filter a correlated subquery is rejected with a regular error, not a LOGICAL_ERROR.
ALTER TABLE t_alias_in_set DELETE WHERE 'YES' IS DISTINCT FROM (SELECT label); -- { serverError NOT_IMPLEMENTED }

-- Mutation predicate references ALIAS -> ALIAS -> IN (MutationsInterpreter).
ALTER TABLE t_alias_in_set DELETE WHERE label = 'YES';
SELECT 'after delete', id, is_special, label FROM t_alias_in_set ORDER BY id;

-- Mutation update value references ALIAS -> ALIAS -> IN (MutationsInterpreter).
ALTER TABLE t_alias_in_set UPDATE category = if(is_special, 'clothing', 'food') WHERE id = 2;
SELECT 'after update', id, is_special, label FROM t_alias_in_set ORDER BY id;

-- Lightweight DELETE goes through the same mutation preparation.
DELETE FROM t_alias_in_set WHERE label = 'YES';
SELECT 'after lightweight delete', count() FROM t_alias_in_set;

ALTER TABLE t_alias_in_set DROP COLUMN IF EXISTS nonexistent_col;

DROP TABLE t_alias_in_set;
