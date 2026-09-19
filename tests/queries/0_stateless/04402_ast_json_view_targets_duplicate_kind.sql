-- Regression test for the AST JSON review hardening of `ASTViewTargets`.
-- The SQL parser builds the `targets` list through setters (`setTableID`, `setInnerEngine`,
-- `setInnerColumns`, ...) that merge by `ViewTarget::Kind`, so it can never produce two targets of
-- the same kind. A `clickhouse_json` AST could append duplicates, which would make formatting
-- (`tryGetTarget` returns only the first match) disagree with execution and access checks (which
-- iterate every entry in `targets`): a second target could be hidden from the formatted SQL while
-- still affecting access collection or replicated-engine detection. `ASTViewTargets::readJSON` now
-- rejects duplicate kinds with `BAD_ARGUMENTS`, matching the parser-produced one-target-per-kind shape.

-- A single `TO` target round-trips unchanged (must NOT be rejected):
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'));

-- A second `To` target prepended to the `targets` array is parser-impossible and is rejected at the
-- JSON boundary (the duplicate is detected when the original `To` target is reached):
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW mv TO dst AS SELECT 1'), '"targets":[', '"targets":[{"kind":"To","table_name":"dup"},')); -- { serverError BAD_ARGUMENTS }
