DROP TABLE IF EXISTS t_subcolumns_if;

CREATE TABLE t_subcolumns_if (id Nullable(Int64)) ENGINE=MergeTree ORDER BY tuple();

INSERT INTO t_subcolumns_if SELECT number::Nullable(Int64) as number FROM numbers(10000);

SELECT
    sum(multiIf(id IS NOT NULL, 1, 0))
FROM t_subcolumns_if
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;

SELECT
    sum(multiIf(id IS NULL, 1, 0))
FROM t_subcolumns_if
SETTINGS enable_analyzer = 0, optimize_functions_to_subcolumns = 1;

SELECT
    sum(multiIf(id IS NULL, 1, 0))
FROM t_subcolumns_if
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 0;

SELECT
    sum(multiIf(id IS NULL, 1, 0))
FROM t_subcolumns_if
SETTINGS enable_analyzer = 1, optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_subcolumns_if;
