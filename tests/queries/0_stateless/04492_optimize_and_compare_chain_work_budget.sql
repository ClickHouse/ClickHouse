-- The `optimize_and_compare_chain` optimization derives transitive predicates from an AND-chain of
-- comparisons (e.g. `(a < b) AND (b < c) AND (c < 5)` also implies `b < 5` and `a < 5`). Its work is
-- bounded by the `optimize_and_compare_chain_max_hash_work` budget, measured in query-tree nodes
-- hashed; once exceeded the optimization backs off for the rest of the query. Backing off only
-- forgoes the (redundant) derived predicates, so query results must never depend on the budget.

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

-- The budget also bounds the DFS that chains comparisons: with the same operands, a long chain backs off where a short one fits.
SELECT
    (SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_compare_chain_budget WHERE (a + 1) < 101 AND (a + 2) < 102 AND (a + 3) < 103 AND (a + 4) < 104 AND (a + 1) > c AND (a + 2) > c AND (a + 3) > c AND (a + 4) > c AND b > (b - 1) AND (b - 1) > (b - 2) AND (b - 2) > (b - 3) AND (b - 3) > (b - 4) AND (b - 4) > (b - 5) AND (b - 5) > (b - 6) AND (b - 6) > (b - 7) AND (b - 7) > (b - 8) AND (b - 8) > (b - 9) AND (b - 9) > (b - 10) AND (b - 10) > (b - 11) AND (b - 11) > (b - 12) AND (b - 12) > (b - 13) AND (b - 13) > (b - 14) AND (b - 14) > (b - 15) AND (b - 15) > (b - 16) AND (b - 16) > (b - 17) AND (b - 17) > (b - 18) AND (b - 18) > (b - 19) AND (b - 19) > (b - 20) AND (b - 20) > (b - 21) AND (b - 21) > (b - 22) AND (b - 22) > (b - 23) AND (b - 23) > (b - 24) AND (b - 24) > (b - 25) AND (b - 25) > (b - 26) AND (b - 26) > (b - 27) AND (b - 27) > (b - 28) AND (b - 28) > (b - 29) AND (b - 29) > (b - 30) AND (b - 30) > (b - 31) AND (b - 31) > (b - 32) AND (b - 32) > (b - 33) AND (b - 33) > (b - 34) AND (b - 34) > (b - 35) AND (b - 35) > (b - 36) AND (b - 36) > (b - 37) AND (b - 37) > (b - 38) AND (b - 38) > (b - 39) AND (b - 39) > (b - 40) SETTINGS optimize_and_compare_chain_max_hash_work = 1900) WHERE explain LIKE '%function_name: less,%')
  > (SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_compare_chain_budget WHERE (a + 1) < 101 AND (a + 2) < 102 AND (a + 3) < 103 AND (a + 4) < 104 AND (a + 1) > c AND (a + 2) > c AND (a + 3) > c AND (a + 4) > c AND b > (b - 1) AND (b - 1) > (b - 2) AND (b - 2) > (b - 3) AND (b - 3) > (b - 4) AND (b - 4) > (b - 5) AND (b - 5) > (b - 6) AND (b - 6) > (b - 7) AND (b - 7) > (b - 8) AND (b - 8) > (b - 9) AND (b - 9) > (b - 10) AND (b - 10) > (b - 11) AND (b - 11) > (b - 12) AND (b - 12) > (b - 13) AND (b - 13) > (b - 14) AND (b - 14) > (b - 15) AND (b - 15) > (b - 16) AND (b - 16) > (b - 17) AND (b - 17) > (b - 18) AND (b - 18) > (b - 19) AND (b - 19) > (b - 20) AND (b - 20) > (b - 21) AND (b - 21) > (b - 22) AND (b - 22) > (b - 23) AND (b - 23) > (b - 24) AND (b - 24) > (b - 25) AND (b - 25) > (b - 26) AND (b - 26) > (b - 27) AND (b - 27) > (b - 28) AND (b - 28) > (b - 29) AND (b - 29) > (b - 30) AND (b - 30) > (b - 31) AND (b - 31) > (b - 32) AND (b - 32) > (b - 33) AND (b - 33) > (b - 34) AND (b - 34) > (b - 35) AND (b - 35) > (b - 36) AND (b - 36) > (b - 37) AND (b - 37) > (b - 38) AND (b - 38) > (b - 39) AND (b - 39) > (b - 40) SETTINGS optimize_and_compare_chain_max_hash_work = 1) WHERE explain LIKE '%function_name: less,%'),
    (SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_compare_chain_budget WHERE (a + 1) < 101 AND (a + 2) < 102 AND (a + 3) < 103 AND (a + 4) < 104 AND (a + 1) > b AND (a + 2) > b AND (a + 3) > b AND (a + 4) > b AND b > (b - 1) AND (b - 1) > (b - 2) AND (b - 2) > (b - 3) AND (b - 3) > (b - 4) AND (b - 4) > (b - 5) AND (b - 5) > (b - 6) AND (b - 6) > (b - 7) AND (b - 7) > (b - 8) AND (b - 8) > (b - 9) AND (b - 9) > (b - 10) AND (b - 10) > (b - 11) AND (b - 11) > (b - 12) AND (b - 12) > (b - 13) AND (b - 13) > (b - 14) AND (b - 14) > (b - 15) AND (b - 15) > (b - 16) AND (b - 16) > (b - 17) AND (b - 17) > (b - 18) AND (b - 18) > (b - 19) AND (b - 19) > (b - 20) AND (b - 20) > (b - 21) AND (b - 21) > (b - 22) AND (b - 22) > (b - 23) AND (b - 23) > (b - 24) AND (b - 24) > (b - 25) AND (b - 25) > (b - 26) AND (b - 26) > (b - 27) AND (b - 27) > (b - 28) AND (b - 28) > (b - 29) AND (b - 29) > (b - 30) AND (b - 30) > (b - 31) AND (b - 31) > (b - 32) AND (b - 32) > (b - 33) AND (b - 33) > (b - 34) AND (b - 34) > (b - 35) AND (b - 35) > (b - 36) AND (b - 36) > (b - 37) AND (b - 37) > (b - 38) AND (b - 38) > (b - 39) AND (b - 39) > (b - 40) SETTINGS optimize_and_compare_chain_max_hash_work = 1900) WHERE explain LIKE '%function_name: less,%')
  = (SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM t_compare_chain_budget WHERE (a + 1) < 101 AND (a + 2) < 102 AND (a + 3) < 103 AND (a + 4) < 104 AND (a + 1) > b AND (a + 2) > b AND (a + 3) > b AND (a + 4) > b AND b > (b - 1) AND (b - 1) > (b - 2) AND (b - 2) > (b - 3) AND (b - 3) > (b - 4) AND (b - 4) > (b - 5) AND (b - 5) > (b - 6) AND (b - 6) > (b - 7) AND (b - 7) > (b - 8) AND (b - 8) > (b - 9) AND (b - 9) > (b - 10) AND (b - 10) > (b - 11) AND (b - 11) > (b - 12) AND (b - 12) > (b - 13) AND (b - 13) > (b - 14) AND (b - 14) > (b - 15) AND (b - 15) > (b - 16) AND (b - 16) > (b - 17) AND (b - 17) > (b - 18) AND (b - 18) > (b - 19) AND (b - 19) > (b - 20) AND (b - 20) > (b - 21) AND (b - 21) > (b - 22) AND (b - 22) > (b - 23) AND (b - 23) > (b - 24) AND (b - 24) > (b - 25) AND (b - 25) > (b - 26) AND (b - 26) > (b - 27) AND (b - 27) > (b - 28) AND (b - 28) > (b - 29) AND (b - 29) > (b - 30) AND (b - 30) > (b - 31) AND (b - 31) > (b - 32) AND (b - 32) > (b - 33) AND (b - 33) > (b - 34) AND (b - 34) > (b - 35) AND (b - 35) > (b - 36) AND (b - 36) > (b - 37) AND (b - 37) > (b - 38) AND (b - 38) > (b - 39) AND (b - 39) > (b - 40) SETTINGS optimize_and_compare_chain_max_hash_work = 1) WHERE explain LIKE '%function_name: less,%');

DROP TABLE t_compare_chain_budget;

-- `optimize_and_compare_chain_max_hash_work` was introduced in 26.7, so `compatibility` set to an
-- earlier version must restore the pre-PR behavior where the optimization was uncapped: the budget
-- reverts to `0` (unlimited).
SELECT value FROM system.settings WHERE name = 'optimize_and_compare_chain_max_hash_work' SETTINGS compatibility = '26.6';
