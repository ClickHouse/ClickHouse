-- Validation for `formatQueryFromJSON` payloads where `formatImpl` /
-- `formatQueryImpl` later unconditionally dereferences fields. Malformed input
-- must throw `BAD_ARGUMENTS` instead of producing an AST that crashes during
-- formatting.

-- AlterQuery: `command_list` is required.
SELECT formatQueryFromJSON('{"type":"AlterQuery","alter_object":"TABLE","table":"t"}'); -- { serverError BAD_ARGUMENTS }

-- DropQuery: at least one of database/table/database_and_tables is required.
SELECT formatQueryFromJSON('{"type":"DropQuery","kind":"Drop"}'); -- { serverError BAD_ARGUMENTS }

-- OptimizeQuery: `table` is required.
SELECT formatQueryFromJSON('{"type":"OptimizeQuery"}'); -- { serverError BAD_ARGUMENTS }

-- InsertQuery: must specify at least one target.
SELECT formatQueryFromJSON('{"type":"InsertQuery"}'); -- { serverError BAD_ARGUMENTS }

-- ConstraintDeclaration: `expr` is required.
SELECT formatQueryFromJSON('{"type":"ConstraintDeclaration","name":"c","constraint_type":"CHECK"}'); -- { serverError BAD_ARGUMENTS }

-- StatisticsDeclaration: `columns` is required.
SELECT formatQueryFromJSON('{"type":"StatisticsDeclaration"}'); -- { serverError BAD_ARGUMENTS }

-- CreateQuery: at least one of `database` or `table` is required.
SELECT formatQueryFromJSON('{"type":"CreateQuery"}'); -- { serverError BAD_ARGUMENTS }

-- RenameQuery: each element requires non-empty from_table/to_table.
SELECT formatQueryFromJSON('{"type":"RenameQuery","elements":[{"from_table":"","to_table":"","from_database":"","to_database":"","if_exists":false}]}'); -- { serverError BAD_ARGUMENTS }

-- RenameQuery (database form): at least one element is required.
SELECT formatQueryFromJSON('{"type":"RenameQuery","database":true,"elements":[]}'); -- { serverError BAD_ARGUMENTS }

-- SystemQuery (SCHEDULE MERGE): requires `table` and `scheduled_merge_parts`.
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":98}'); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":98,"table":{"type":"Identifier","name":"t"}}'); -- { serverError BAD_ARGUMENTS }

-- SystemQuery (REFRESH VIEW): requires `table`.
SELECT formatQueryFromJSON('{"type":"SystemQuery","query_type":102}'); -- { serverError BAD_ARGUMENTS }

-- Well-formed payloads still work.
SELECT formatQueryFromJSON(parseQueryToJSON('OPTIMIZE TABLE t'));
SELECT formatQueryFromJSON(parseQueryToJSON('DROP TABLE t'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE DATABASE d'));
