-- The only non-view / non-`TimeSeries` `CREATE` form that carries `targets` is a plain table with a
-- `TO INNER UUID` clause. `ParserCreateQuery` builds it only for `SharedSet` / `SharedJoin` engines, and
-- the resulting `ASTViewTargets` holds exactly one `To` target whose sole payload is `inner_uuid`.
-- `ASTCreateQuery::readJSON` must therefore validate the target shape and the engine, not just the presence
-- of `has_inner_uuid_clause`; otherwise `formatQueryImpl` could emit SQL the parser never accepts, such as
-- `CREATE TABLE t TO dst` or a `TO INNER UUID` table on an engine that does not support it.

-- Valid inner-UUID tables round-trip unchanged:
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t TO INNER UUID ''00000000-0000-0000-0000-000000000001'' (x UInt32) ENGINE = SharedSet(''/z'', ''r'')'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t TO INNER UUID ''00000000-0000-0000-0000-000000000001'' (x UInt32) ENGINE = SharedJoin(''/z'', ''r'', ANY, LEFT, x)'));

-- An external `TO dst` target smuggled in through the `has_inner_uuid_clause` escape hatch: the shape is not
-- the single inner-UUID target the parser produces, so it must be rejected instead of formatting `CREATE TABLE t TO dst`.
SELECT formatQueryFromJSON('{"type":"CreateQuery","table":"t","has_inner_uuid_clause":true,"targets":{"type":"ViewTargets","targets":[{"kind":"To","table_name":"dst"}]}}'); -- { serverError BAD_ARGUMENTS }

-- A valid inner-UUID target shape but on an engine that does not support inner UUID:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t TO INNER UUID ''00000000-0000-0000-0000-000000000001'' (x UInt32) ENGINE = SharedSet(''/z'', ''r'')'), '"SharedSet"', '"Memory"')); -- { serverError BAD_ARGUMENTS }
