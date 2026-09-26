-- Dictionary `RANGE` / `LIFETIME` / `LAYOUT` clauses and `TimeInterval` are leaf nodes whose scalars
-- are always written to JSON and always printed back by `formatImpl`. A missing key must be rejected
-- instead of silently defaulting to an empty name or a zero bound, which would either format a
-- parser-impossible clause or produce a different, valid definition.

SET enable_json_ast_dialect = 1;

-- Positives: the clauses round-trip byte-identically.

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(300)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(300)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT) LIFETIME(MIN 0 MAX 0)$$))
    = formatQuerySingleLine($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(FLAT) LIFETIME(MIN 0 MAX 0)$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY OFFSET 1 HOUR RANDOMIZE FOR 1 MINUTE APPEND TO dst AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY OFFSET 1 HOUR RANDOMIZE FOR 1 MINUTE APPEND TO dst AS SELECT 1$$);

SELECT formatQueryFromJSON(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH AFTER 0 SECOND TO dst AS SELECT 1$$))
    = formatQuerySingleLine($$CREATE MATERIALIZED VIEW mv REFRESH AFTER 0 SECOND TO dst AS SELECT 1$$);

-- Negatives: dropping a required key must fail at the JSON boundary.

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"min_attr_name":"a"', '"unused_min_attr_name":"a"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"max_attr_name":"b"', '"unused_max_attr_name":"b"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"min_attr_name":"a"', '"min_attr_name":""')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"min_sec":1', '"unused_min_sec":1')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"max_sec":10', '"unused_max_sec":10')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"layout_type":"range_hashed"', '"unused_layout_type":"range_hashed"')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64, a UInt64, b UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(RANGE_HASHED()) RANGE(MIN a MAX b) LIFETIME(MIN 1 MAX 10)$$),
    '"layout_type":"range_hashed"', '"layout_type":""')); -- { serverError BAD_ARGUMENTS }

-- The parser rejects layout parameters that are not enclosed in brackets.
SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE DICTIONARY d (id UInt64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't')) LAYOUT(CACHE(SIZE_IN_CELLS 10)) LIFETIME(300)$$),
    '"layout_type":"cache","has_brackets":true', '"layout_type":"cache","has_brackets":false')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY OFFSET 1 HOUR RANDOMIZE FOR 1 MINUTE APPEND TO dst AS SELECT 1$$),
    '"seconds":3600', '"unused_seconds":3600')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 DAY OFFSET 1 HOUR RANDOMIZE FOR 1 MINUTE APPEND TO dst AS SELECT 1$$),
    '"seconds":86400,"months":0', '"seconds":86400')); -- { serverError BAD_ARGUMENTS }

SELECT formatQueryFromJSON(replace(parseQueryToJSON($$CREATE MATERIALIZED VIEW mv REFRESH AFTER 1 HOUR TO dst AS SELECT 1$$),
    '"type":"TimeInterval","seconds":3600,"months":0', '"type":"TimeInterval"')); -- { serverError BAD_ARGUMENTS }
