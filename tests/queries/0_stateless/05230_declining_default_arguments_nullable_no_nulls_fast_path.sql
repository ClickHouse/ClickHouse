-- A function that declines `canBeExecutedOnDefaultArguments` is not executed on the rows behind a
-- NULL: they are filtered out first. The filtering is only needed when the input really contains a
-- NULL - a `Nullable` argument without a single NULL must keep the numeric fast path of
-- `defaultImplementationForNulls`, which does not filter and does not account the rows in
-- `DefaultImplementationForNullsRows`.
-- https://github.com/ClickHouse/ClickHouse/pull/117359

SELECT 'without a NULL the fast path is kept';
SELECT sum(intDiv(x, 2)) FROM (SELECT number::Nullable(UInt64) AS x FROM numbers(1000))
SETTINGS log_comment = 'declining_no_nulls';

SELECT 'with a NULL the rows are filtered out';
SELECT sum(intDiv(x, 2)) FROM (SELECT if(number = 0, NULL, number)::Nullable(UInt64) AS x FROM numbers(1000))
SETTINGS log_comment = 'declining_with_nulls';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['DefaultImplementationForNullsRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment IN ('declining_no_nulls', 'declining_with_nulls')
ORDER BY ALL;
