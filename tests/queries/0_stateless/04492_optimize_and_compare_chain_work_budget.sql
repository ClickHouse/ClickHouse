-- The `optimize_and_compare_chain` optimization derives transitive predicates from an AND-chain of
-- comparisons (e.g. `(a < b) AND (b < c) AND (c < 5)` also implies `b < 5` and `a < 5`). Its work is
-- bounded by the `optimize_and_compare_chain_max_hash_work` budget, measured in query-tree nodes
-- hashed; once exceeded the optimization backs off for the rest of the query. Backing off only
-- forgoes the (redundant) derived predicates, so query results must never depend on the budget.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS t_compare_chain_budget;
CREATE TABLE t_compare_chain_budget (a Int32, b Int32, c Int32) ENGINE = Memory;
INSERT INTO t_compare_chain_budget VALUES (1, 2, 3), (1, 2, 9), (4, 5, 6), (10, 11, 12), (0, 1, 2);

-- Results are identical across: the default budget, a budget of 1 (forces the optimization to back
-- off, including during operand collection), an unlimited budget (0), and the optimization disabled.
SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) ORDER BY a, b, c SETTINGS optimize_and_compare_chain_max_hash_work = 5000000;
SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) ORDER BY a, b, c SETTINGS optimize_and_compare_chain_max_hash_work = 1;
SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) ORDER BY a, b, c SETTINGS optimize_and_compare_chain_max_hash_work = 0;
SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) ORDER BY a, b, c SETTINGS optimize_and_compare_chain = 0;

-- The budget actually controls whether the optimization fires: with a large budget the transitive
-- chain expands (more `less` comparisons appear in the analyzed query tree), while a budget of 1
-- makes it back off and the chain is left as written. So the large-budget tree has strictly more
-- `less` comparisons than the budget-of-1 tree (printed as 1). Asserting the relationship rather
-- than absolute counts keeps the test robust to unrelated analyzer settings.
SELECT
    (SELECT count() FROM (EXPLAIN QUERY TREE SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) SETTINGS optimize_and_compare_chain_max_hash_work = 5000000) WHERE explain LIKE '%function_name: less,%')
  > (SELECT count() FROM (EXPLAIN QUERY TREE SELECT a, b, c FROM t_compare_chain_budget WHERE (a < b) AND (b < c) AND (c < 5) SETTINGS optimize_and_compare_chain_max_hash_work = 1) WHERE explain LIKE '%function_name: less,%');

DROP TABLE t_compare_chain_budget;

-- `optimize_and_compare_chain_max_hash_work` was introduced in 26.7, so `compatibility` set to an
-- earlier version must restore the pre-PR behavior where the optimization was uncapped: the budget
-- reverts to `0` (unlimited).
SELECT value FROM system.settings WHERE name = 'optimize_and_compare_chain_max_hash_work' SETTINGS compatibility = '26.6';
