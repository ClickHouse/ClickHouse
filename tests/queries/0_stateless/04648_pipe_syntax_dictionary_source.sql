-- A `ClickHouse` dictionary source query is a top-level query of its own, and it is re-parsed for
-- dependency extraction (`DDLDependencyVisitor`) and for validation
-- (`ClickHouseDictionarySource::createStreamForQuery`) before it is executed. Neither may reject a
-- pipe source query that the execution path accepts. The dictionary source context is built from the
-- server defaults plus the dictionary's own `SETTINGS(...)` clause, not from the session, so that is
-- where `allow_experimental_pipe_syntax` has to be enabled.
DROP DICTIONARY IF EXISTS dict_pipe;

CREATE DICTIONARY dict_pipe (k UInt64, v UInt64)
PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'FROM numbers(4) |> WHERE number > 0 |> AGGREGATE number AS k, sum(number * 10) AS v GROUP BY k'))
LAYOUT(FLAT())
LIFETIME(0)
SETTINGS(allow_experimental_pipe_syntax = 1);

SELECT dictGet('dict_pipe', 'v', toUInt64(2));
SELECT dictGet('dict_pipe', 'v', toUInt64(3));
-- Row 0 is filtered out by the pipe `WHERE`, so the dictionary returns the default value.
SELECT dictHas('dict_pipe', toUInt64(0)), dictGet('dict_pipe', 'v', toUInt64(0));

DROP DICTIONARY dict_pipe;

-- Without the setting the source query is rejected when the dictionary is loaded, as at any other
-- entry point. The `CREATE` itself succeeds: dependency extraction does not enforce the setting.
CREATE DICTIONARY dict_pipe (k UInt64, v UInt64)
PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'FROM numbers(4) |> WHERE number > 0 |> AGGREGATE number AS k, sum(number * 10) AS v GROUP BY k'))
LAYOUT(FLAT())
LIFETIME(0);

SELECT dictGet('dict_pipe', 'v', toUInt64(2)); -- { serverError SYNTAX_ERROR }

DROP DICTIONARY dict_pipe;
