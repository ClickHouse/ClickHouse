-- Tags: need-query-parameters
-- Short ATTACH TABLE into a database that does not exist must report UNKNOWN_DATABASE.
-- CLICKHOUSE_DATABASE_1 is per-test unique and never created by the runner.
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t; -- { serverError UNKNOWN_DATABASE }
