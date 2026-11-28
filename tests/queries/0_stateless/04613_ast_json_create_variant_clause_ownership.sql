-- `ASTCreateQuery::readJSON` must validate which `CREATE` variant owns each clause family:
-- the parser attaches `refresh_strategy` only to materialized views, and `targets` only to
-- materialized views, `TimeSeries` tables, or tables with a `TO INNER UUID` clause. Malformed
-- `clickhouse_json` attaching them elsewhere must fail with `BAD_ARGUMENTS` instead of
-- formatting parser-impossible SQL like `CREATE TABLE t TO dst` or `CREATE TABLE t REFRESH`.

-- Valid shapes that the validation must NOT reject (round-trip unchanged):
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v TO dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR APPEND TO dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries DATA db.d TAGS db.t METRICS db.m'));

-- `targets` on a plain `CREATE TABLE` (materialized-view flag stripped off a `TO` view):
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v TO dst AS SELECT 1'), '"is_materialized_view":true', '"is_materialized_view":false')); -- { serverError BAD_ARGUMENTS }

-- `refresh_strategy` outside a materialized view:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR APPEND TO dst AS SELECT 1'), '"is_materialized_view":true', '"is_materialized_view":false')); -- { serverError BAD_ARGUMENTS }
