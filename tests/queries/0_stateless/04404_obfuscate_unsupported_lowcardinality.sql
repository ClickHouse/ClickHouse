-- The obfuscator shares its model code with the `clickhouse obfuscator` tool and currently does not
-- support `LowCardinality` columns. Instead of crashing it rejects them with a clean NOT_IMPLEMENTED
-- exception. Real support for `LowCardinality` is tracked as a follow-up so that the table function
-- and the CLI tool stay in sync.

SELECT * FROM obfuscate(SELECT toLowCardinality('x') AS s) SETTINGS obfuscate_seed = 'seed'; -- { serverError NOT_IMPLEMENTED }
