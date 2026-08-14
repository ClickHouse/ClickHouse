-- Regression test: KILL QUERY/MUTATION TEST should not throw
-- "Bad cast from type X to Y" when the internal SELECT returns columns
-- that need materialization (e.g. ColumnConst).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=98138&sha=a349e29ce98d34354439478bec564adb7d5b66c9&name_0=PR&name_1=Stress%20test%20%28amd_msan%29

SELECT 'KILL QUERY';
KILL QUERY WHERE 1 TEST FORMAT Null;
KILL QUERY WHERE user = currentUser() TEST FORMAT Null;
KILL QUERY WHERE query_id = 'nonexistent_query_id' TEST FORMAT Null;

SELECT 'KILL MUTATION';
KILL MUTATION WHERE 1 TEST FORMAT Null;
KILL MUTATION WHERE database = currentDatabase() TEST FORMAT Null;
KILL MUTATION WHERE mutation_id = 'nonexistent_mutation_id' TEST FORMAT Null;

-- KILL TRANSACTION is not tested here because system.transactions
-- only exists when experimental transactions are enabled.
