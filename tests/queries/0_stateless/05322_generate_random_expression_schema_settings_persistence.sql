-- An unseeded `generateRandomStructure` expression is resolved once, while `SETTINGS` remain in the definition.
DROP TABLE IF EXISTS t_generate_random_expression;
DROP TABLE IF EXISTS t_generate_random_expression_snapshot;

CREATE TABLE t_generate_random_expression AS generateRandom(
    concat('probe Nullable(UInt8), ', generateRandomStructure(3)), 43, 2, 1, SETTINGS null_ratio = 1);

CREATE TABLE t_generate_random_expression_snapshot ENGINE = Memory AS
SELECT create_table_query AS definition
FROM system.tables
WHERE database = currentDatabase() AND name = 't_generate_random_expression';

SELECT
    position(definition, 'concat(') = 0,
    position(definition, 'generateRandomStructure(') = 0,
    position(definition, 'SETTINGS null_ratio = 1') > 0
FROM t_generate_random_expression_snapshot;

SELECT countIf(isNull(probe)), count()
FROM (SELECT probe FROM t_generate_random_expression LIMIT 100)
SETTINGS max_threads = 1, max_block_size = 64, preferred_block_size_bytes = 0;

-- The stored literal schema and `SETTINGS` survive `DETACH` and `ATTACH` together.
DETACH TABLE t_generate_random_expression;
ATTACH TABLE t_generate_random_expression;

SELECT
    create_table_query = (SELECT definition FROM t_generate_random_expression_snapshot),
    position(create_table_query, 'SETTINGS null_ratio = 1') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_generate_random_expression';

SELECT countIf(isNull(probe)), count()
FROM (SELECT probe FROM t_generate_random_expression LIMIT 100)
SETTINGS max_threads = 1, max_block_size = 64, preferred_block_size_bytes = 0;

DROP TABLE t_generate_random_expression;
DROP TABLE t_generate_random_expression_snapshot;
