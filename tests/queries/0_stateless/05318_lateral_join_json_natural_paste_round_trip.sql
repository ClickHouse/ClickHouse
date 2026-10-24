-- Tags: no-fasttest
-- no-fasttest: `formatQueryFromJSON` is not built in the fast test.

-- The parser accepts `NATURAL ... JOIN LATERAL` and `PASTE JOIN LATERAL` without `ON`/`USING`,
-- so their `clickhouse_json` form must round-trip as well.
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t1 NATURAL LEFT JOIN LATERAL (SELECT 1) AS s'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t1 PASTE JOIN LATERAL (SELECT 1) AS s'));
-- Other `LATERAL` joins still require a predicate (see `05293_lateral_join_table_function_group_by_limit_json`).
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT * FROM t1 LEFT JOIN LATERAL (SELECT 1) AS s ON true'));
