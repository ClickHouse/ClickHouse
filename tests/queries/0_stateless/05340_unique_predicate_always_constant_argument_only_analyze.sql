-- In only-analyze mode (`CREATE VIEW`, `EXPLAIN`) the `UNIQUE` predicate is not executed and resolves to a
-- non-constant placeholder. Function arguments that must be constant (e.g. the index of `tupleElement`)
-- need the real value, the same as in execution.

DROP VIEW IF EXISTS v_unique_tuple_element;
CREATE VIEW v_unique_tuple_element AS SELECT tupleElement((1, 2), UNIQUE(SELECT number FROM numbers(3))) AS x;
SELECT * FROM v_unique_tuple_element;
DROP VIEW v_unique_tuple_element;

SELECT tupleElement((1, 2), UNIQUE(SELECT number FROM numbers(3)));
SELECT count() > 0 FROM (EXPLAIN SELECT tupleElement((1, 2), UNIQUE(SELECT number FROM numbers(3))));
SELECT count() > 0 FROM (EXPLAIN SELECT tupleElement((10, 20), 1 + UNIQUE((SELECT 1 UNION ALL SELECT 1))));
