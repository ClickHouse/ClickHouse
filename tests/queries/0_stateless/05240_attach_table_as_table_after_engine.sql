-- `ATTACH TABLE t ENGINE = E AS other` parses (the `AS other` clause after the storage), but the formatter wrote
-- `ATTACH TABLE t AS other ENGINE = E`, which the parser reads as the `ATTACH TABLE t AS [NOT] REPLICATED` form and
-- rejects. Found by json_ast_sql_parser_fuzzer (JSON_AST_FUZZER_STRICT=reparse).
SELECT formatQuerySingleLine('ATTACH TABLE l ENGINE = Memory AS t');
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE l ENGINE = Memory AS t SETTINGS x = 1'));
SELECT formatQuerySingleLine('ATTACH TABLE l ENGINE = TimeSeries() AS db.t SETTINGS x = 864000');
-- CREATE keeps the usual order.
SELECT formatQuerySingleLine('CREATE TABLE l ENGINE = Memory AS t');
SELECT formatQuerySingleLine('CREATE TABLE l AS t ENGINE = Memory');
SELECT formatQuerySingleLine('ATTACH TABLE l AS REPLICATED');
SELECT formatQuerySingleLine('ATTACH TABLE l AS NOT REPLICATED');

-- With target clauses the AS clause follows them, because the parser consumes the targets right after the engine.
SELECT formatQuerySingleLine('ATTACH TABLE l ENGINE = TimeSeries TAGS tg AS t');
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE l ENGINE = TimeSeries DATA dt TAGS tg METRICS mt AS t SETTINGS x = 1'));
