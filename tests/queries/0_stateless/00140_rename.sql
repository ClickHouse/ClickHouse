-- Tags: stateful, no-replicated-database
-- Tag no-replicated-database: Does not support renaming of multiple tables in single query

-- Local fixtures instead of renaming the shared stateful tables. Only the two
-- counters the assertions read are copied. `CLONE AS` is not usable here: it
-- inherits the source table's storage, which is static (read-only) when the
-- stateful tables are attached from a web disk.
CREATE TABLE hits ENGINE = MergeTree ORDER BY CounterID AS SELECT CounterID FROM test.hits WHERE CounterID = 732797;
CREATE TABLE visits ENGINE = MergeTree ORDER BY CounterID AS SELECT CounterID, Sign FROM test.visits WHERE CounterID = 912887;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

RENAME TABLE hits TO visits_tmp, visits TO hits, visits_tmp TO visits;

SELECT sum(Sign) FROM hits WHERE CounterID = 912887;
SELECT count() FROM visits WHERE CounterID = 732797;

RENAME TABLE hits TO hits_tmp, hits_tmp TO hits;

SELECT sum(Sign) FROM hits WHERE CounterID = 912887;
SELECT count() FROM visits WHERE CounterID = 732797;

RENAME TABLE hits TO visits_tmp, visits TO hits, visits_tmp TO visits;

SELECT count() FROM hits WHERE CounterID = 732797;
SELECT sum(Sign) FROM visits WHERE CounterID = 912887;

RENAME TABLE hits TO hits2, hits2 TO hits3, hits3 TO hits4, hits4 TO hits5, hits5 TO hits6, hits6 TO hits7, hits7 TO hits8, hits8 TO hits9, hits9 TO hits10;

SELECT count() FROM hits10 WHERE CounterID = 732797;

RENAME TABLE hits10 TO hits;

SELECT count() FROM hits WHERE CounterID = 732797;

RENAME TABLE hits TO {CLICKHOUSE_DATABASE_1:Identifier}.hits, visits TO hits;

SELECT sum(Sign) FROM hits WHERE CounterID = 912887;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.hits WHERE CounterID = 732797;

RENAME TABLE hits TO visits, {CLICKHOUSE_DATABASE_1:Identifier}.hits TO hits;

SELECT count() FROM hits WHERE CounterID = 732797;
SELECT sum(Sign) FROM visits WHERE CounterID = 912887;

DROP TABLE hits;
DROP TABLE visits;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
