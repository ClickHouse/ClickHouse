-- `ASTCreateQuery::readJSON` must validate which `CREATE` variant owns each clause family:
-- the parser attaches `refresh_strategy` only to materialized views, the watermark strategies
-- and `ALLOWED LATENESS` only to window views, and `targets` only to materialized views,
-- window views, `TimeSeries` tables, or tables with a `TO INNER UUID` clause. Malformed
-- `clickhouse_json` attaching them elsewhere must fail with `BAD_ARGUMENTS` instead of
-- formatting parser-impossible SQL like `CREATE TABLE t TO dst` or `CREATE TABLE t REFRESH`.

-- Valid shapes that the validation must NOT reject (round-trip unchanged):
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v TO dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR APPEND TO dst AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t ENGINE = TimeSeries DATA db.d TAGS db.t METRICS db.m'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE WINDOW VIEW wv ENGINE = Memory WATERMARK = ASCENDING AS SELECT count() FROM t GROUP BY tumble(now(), toIntervalSecond(1))'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE WINDOW VIEW wv TO dst WATERMARK = INTERVAL 3 SECOND ALLOWED_LATENESS INTERVAL 5 SECOND AS SELECT count() FROM t GROUP BY tumble(now(), toIntervalSecond(1))'));

-- `targets` on a plain `CREATE TABLE` (materialized-view flag stripped off a `TO` view):
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v TO dst AS SELECT 1'), '"is_materialized_view":true', '"is_materialized_view":false')); -- { serverError BAD_ARGUMENTS }

-- `refresh_strategy` outside a materialized view:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE MATERIALIZED VIEW v REFRESH EVERY 1 HOUR APPEND TO dst AS SELECT 1'), '"is_materialized_view":true', '"is_materialized_view":false')); -- { serverError BAD_ARGUMENTS }

-- A watermark strategy on a plain table:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'), '"is_watermark_ascending":false', '"is_watermark_ascending":true')); -- { serverError BAD_ARGUMENTS }

-- `ALLOWED LATENESS` on a plain table:
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'), '"allowed_lateness":false', '"allowed_lateness":true')); -- { serverError BAD_ARGUMENTS }
