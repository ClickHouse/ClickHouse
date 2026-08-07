-- `indexHint` hides its subquery set from the early set build, so a column that
-- `system.databases` filters on but does not declare in getFilterSampleBlock used to reach
-- execution with an unbuilt set: "Not-ready Set is passed". Needs all three of indexHint,
-- an undeclared column and a subquery RHS.
SELECT count() FROM system.databases WHERE indexHint(name IN (SELECT currentDatabase()));
SELECT count() FROM system.databases WHERE indexHint(name IN (SELECT 'db_that_does_not_exist_04772'));
SELECT count() > 0 FROM system.databases WHERE indexHint(name NOT IN (SELECT 'db_that_does_not_exist_04772'));
-- Control: `engine` was already declared, so this passed before the fix too.
SELECT count() > 0 FROM system.databases WHERE indexHint(engine IN (SELECT engine FROM system.databases));
