-- `CLONE` is a parser-owned flag of `ParserCreateTableQuery`: it is set nowhere else, it is accepted
-- only for tables, and the parser requires an `AS <source>` right after it. `ASTCreateQuery::formatQueryImpl`
-- prints ` CLONE` for every shape that carries a source, so a `clickhouse_json` payload that sets
-- `is_clone_as` on a view / dictionary form, on a source-less table, or together with `EMPTY`, would
-- format into DDL that no SQL parser can read back (and `InterpreterCreateQuery` branches on the flag to
-- attach the source partitions). Such shapes must be rejected at the JSON boundary.

-- Parser-produced `CLONE` shapes round-trip.
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t CLONE AS src'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t CLONE AS SELECT 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t CLONE AS numbers(10)'));
SELECT formatQueryFromJSON(parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory CLONE AS SELECT 1'));

-- An ordinary view never carries `CLONE`.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE VIEW v AS SELECT 1'),
    '"is_clone_as":false',
    '"is_clone_as":true')); -- { serverError BAD_ARGUMENTS }

-- Neither does a materialized view.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE MATERIALIZED VIEW mv ENGINE = Memory AS SELECT 1'),
    '"is_clone_as":false',
    '"is_clone_as":true')); -- { serverError BAD_ARGUMENTS }

-- Nor a dictionary.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE DICTIONARY d (`id` UInt64) PRIMARY KEY id SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)'),
    '"is_clone_as":false',
    '"is_clone_as":true')); -- { serverError BAD_ARGUMENTS }

-- A table without any source has nothing to clone from.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory'),
    '"is_clone_as":false',
    '"is_clone_as":true')); -- { serverError BAD_ARGUMENTS }

-- `EMPTY` and `CLONE` are mutually exclusive for the parser.
SELECT formatQueryFromJSON(replace(
    parseQueryToJSON('CREATE TABLE t (`x` UInt8) ENGINE = Memory EMPTY AS SELECT 1'),
    '"is_clone_as":false',
    '"is_clone_as":true')); -- { serverError BAD_ARGUMENTS }
