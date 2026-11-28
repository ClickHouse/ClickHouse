-- The parser accepts `TTL expr GROUP BY` with no grouping keys and MergeTree accepts the table; parseQueryToJSON
-- wrote it and formatQueryFromJSON rejected its own output. Found by the JSON round-trip stage of
-- json_ast_sql_parser_fuzzer.
SELECT formatQueryFromJSON(parseQueryToJSON('ALTER TABLE t MODIFY TTL d GROUP BY'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, v UInt64) ENGINE = MergeTree ORDER BY d TTL d + INTERVAL 1 DAY GROUP BY'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (d Date, k UInt8, v UInt64) ENGINE = MergeTree ORDER BY (k, d) TTL d + INTERVAL 1 DAY GROUP BY k SET v = sum(v)'));
-- Keys on a mode other than GROUP BY are still rejected.
SELECT formatQueryFromJSON('{"type":"TTLElement","mode":"DELETE","ttl":{"type":"Identifier","name":"d"},"group_by_key":[{"type":"Identifier","name":"k"}]}'); -- { serverError BAD_ARGUMENTS }
