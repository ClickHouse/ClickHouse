-- A TTL `where_expr` is valid only for `DELETE`, the only mode after which the parser accepts `WHERE`.

-- DELETE keeps its WHERE, both parsed from SQL and added to the JSON.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY TTL d WHERE x = 1'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d'), '"ttl_expr":{"type":"Identifier","name":"d"}', '"ttl_expr":{"type":"Identifier","name":"d"},"where_expr":{"type":"Identifier","name":"x"}'));

-- The same WHERE on MOVE, RECOMPRESS and GROUP BY is rejected.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d TO DISK \'fast\''), '"ttl_expr":{"type":"Identifier","name":"d"}', '"ttl_expr":{"type":"Identifier","name":"d"},"where_expr":{"type":"Identifier","name":"x"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d RECOMPRESS CODEC(ZSTD(1))'), '"ttl_expr":{"type":"Identifier","name":"d"}', '"ttl_expr":{"type":"Identifier","name":"d"},"where_expr":{"type":"Identifier","name":"x"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('ALTER TABLE t MODIFY TTL d GROUP BY x SET y = max(y)'), '"ttl_expr":{"type":"Identifier","name":"d"}', '"ttl_expr":{"type":"Identifier","name":"d"},"where_expr":{"type":"Identifier","name":"x"}')); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON(replace(parseQueryToJSON('CREATE TABLE t (d DateTime, x UInt8) ENGINE = MergeTree ORDER BY x TTL d TO DISK \'default\''), '"ttl_expr":{"type":"Identifier","name":"d"}', '"ttl_expr":{"type":"Identifier","name":"d"},"where_expr":{"type":"Identifier","name":"x"}')); -- { serverError BAD_ARGUMENTS }
