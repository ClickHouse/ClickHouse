-- A number literal in the `view` table function and in `EXPLAIN` used as a subquery is typed by its
-- context, the same as in the query itself.

SELECT 'query', toDecimal128('0.1000000000000000000001', 22) > 0.1;
SELECT 'view', * FROM view(SELECT toDecimal128('0.1000000000000000000001', 22) > 0.1);
SELECT 'viewIfPermitted', * FROM viewIfPermitted(SELECT toDecimal128('0.1000000000000000000001', 22) > 0.1 AS c ELSE null('c UInt8'));
SELECT 'explain', trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(3) WHERE toDecimal64(number, 2) = 1.1) WHERE explain LIKE '%Filter column%';

-- With the setting disabled the literal is a `Float64` everywhere.
SET enable_context_typed_number_literals = 0;
SELECT 'query, disabled', toDecimal128('0.1000000000000000000001', 22) > 0.1;
SELECT 'view, disabled', * FROM view(SELECT toDecimal128('0.1000000000000000000001', 22) > 0.1);
SELECT 'explain, disabled', trim(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM numbers(3) WHERE toDecimal64(number, 2) = 1.1) WHERE explain LIKE '%Filter column%';
