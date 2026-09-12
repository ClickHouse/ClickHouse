-- `IF CHANGED` is a semantic part of a refresh strategy and must survive AST JSON round-trips.
SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR IF CHANGED ENGINE = Memory AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR IF CHANGED ENGINE = Memory AS SELECT 1$$);
