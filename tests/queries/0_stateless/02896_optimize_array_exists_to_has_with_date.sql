-- The optimization should not be applied for Date vs String comparison,
-- because has() requires a common supertype which doesn't exist for these types.
-- The query should still work correctly via the original arrayExists with lambda.
SELECT arrayExists(date -> (date = '2022-07-31'), [toDate('2022-07-31')]) AS date_exists;
