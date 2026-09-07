SET enable_analyzer = 1;

-- In only_analyze mode (EXPLAIN, `CREATE VIEW` validation, distributed shard headers) the UNIQUE
-- predicate must not leave a raw non-correlated `QueryNode` in expression position. Otherwise
-- consumers that build projection actions from the analyzed tree (for example
-- `InterpreterSelectQueryAnalyzer::getSampleBlock`) reach it through `PlannerActionsVisitor::visitQuery`,
-- which rejects non-correlated query nodes with "Only correlated QueryNode can be used as an action
-- query tree node". The predicate now folds to a typed `UInt8` constant in only_analyze mode too.

SELECT 'CREATE VIEW validation over UNIQUE';
DROP VIEW IF EXISTS v_unique_only_analyze;
CREATE VIEW v_unique_only_analyze AS SELECT UNIQUE((SELECT number FROM numbers(3))) AS u;
SELECT * FROM v_unique_only_analyze;
DROP VIEW v_unique_only_analyze;

SELECT 'EXPLAIN over UNIQUE does not throw';
SELECT count() >= 1 FROM (EXPLAIN QUERY TREE SELECT UNIQUE((SELECT number FROM numbers(3))));
SELECT count() >= 1 FROM (EXPLAIN PLAN SELECT UNIQUE((SELECT number FROM numbers(3))));

SELECT 'UNIQUE nested inside an expression under CREATE VIEW';
DROP VIEW IF EXISTS v_unique_nested_only_analyze;
CREATE VIEW v_unique_nested_only_analyze AS SELECT 1 + UNIQUE((SELECT number FROM numbers(3))) AS u;
SELECT * FROM v_unique_nested_only_analyze;
DROP VIEW v_unique_nested_only_analyze;
