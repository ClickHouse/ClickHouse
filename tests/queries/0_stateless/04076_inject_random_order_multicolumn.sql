-- Verify that multi-column SELECT works with inject_random_order_for_select_without_order_by=1.
-- Previously this crashed with BAD_ARGUMENTS because only a single alias was generated.
SET inject_random_order_for_select_without_order_by = 1;

-- Multi-column SELECT should succeed
SELECT number, number + 1 FROM numbers(3) FORMAT Null;

-- SELECT * from a table with multiple columns should succeed
SELECT * FROM system.one FORMAT Null;
