-- The SQL backends no longer exist.
SELECT kql_array_sort_asc([1]); -- { serverError UNKNOWN_FUNCTION }
SELECT kql_array_sort_desc([1]); -- { serverError UNKNOWN_FUNCTION }

-- The KQL names are no longer supported in the kusto dialect (rejected by the parser, hence clientError).
SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';
print array_sort_asc(dynamic([1])); -- { clientError UNKNOWN_FUNCTION }
print array_sort_desc(dynamic([1])); -- { clientError UNKNOWN_FUNCTION }
SET dialect = 'clickhouse';

-- #106850: formatting the subscript on a kql_array_sort_desc result must be idempotent
-- (the removed arrayElement -> tupleElement rewrite made the trailing-operator form diverge).
SELECT formatQuerySingleLine('SELECT kql_array_sort_desc([3,1,2])[1]') = formatQuerySingleLine(formatQuerySingleLine('SELECT kql_array_sort_desc([3,1,2])[1]'));
SELECT formatQuerySingleLine('SELECT kql_array_sort_desc([3,1,2])[1] - 1') = formatQuerySingleLine(formatQuerySingleLine('SELECT kql_array_sort_desc([3,1,2])[1] - 1'));
