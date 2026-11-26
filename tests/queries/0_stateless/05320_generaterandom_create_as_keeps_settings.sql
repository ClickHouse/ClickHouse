-- `CREATE TABLE ... AS generateRandom(..., SETTINGS ...)` keeps the `SETTINGS` argument: the table generates data with
-- it, and its stored definition holds it, so it survives `DETACH` / `ATTACH`.

DROP TABLE IF EXISTS t_generate_random_settings;

CREATE TABLE t_generate_random_settings AS generateRandom('a Nullable(UInt8)', 1, 10, 10, SETTINGS null_ratio = 1);

SELECT position(create_table_query, 'SETTINGS null_ratio = 1') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_generate_random_settings';

SELECT countIf(a IS NULL), count() FROM (SELECT * FROM t_generate_random_settings LIMIT 100);

DETACH TABLE t_generate_random_settings;
ATTACH TABLE t_generate_random_settings;

SELECT countIf(a IS NULL), count() FROM (SELECT * FROM t_generate_random_settings LIMIT 100);

DROP TABLE t_generate_random_settings;
