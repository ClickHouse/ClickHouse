-- Test that implicit minmax indices are only created for ALIAS columns with expressions,
-- not for simple column references or alias-of-alias chains.

DROP TABLE IF EXISTS test_alias_reference;

-- Create table with:
-- a: regular column (should get implicit index)
-- b ALIAS a: simple alias to column (should NOT get implicit index)
-- c ALIAS b: alias-of-alias (should NOT get implicit index)
-- d ALIAS a > 0: alias with expression (should get implicit index)
CREATE TABLE test_alias_reference (
    a UInt64,
    b ALIAS a,
    c ALIAS b,
    d ALIAS a > 0
) ENGINE = MergeTree
ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = true;

-- Verify only 'a' and 'd' have implicit indices, not 'b' or 'c'
SELECT name, type, expr
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'test_alias_reference'
ORDER BY name;

-- Verify the table works correctly with all aliases
INSERT INTO test_alias_reference VALUES (10), (20), (0);

SELECT a, b, c, d FROM test_alias_reference ORDER BY a;

DROP TABLE test_alias_reference;
