DROP DICTIONARY IF EXISTS 02843_dict;
DROP TABLE IF EXISTS 02843_source;
DROP TABLE IF EXISTS 02843_join;

CREATE TABLE 02843_source
(
  id UInt64,
  value String
)
ENGINE=Memory;

CREATE DICTIONARY 02843_dict
(
    id UInt64,
    value String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE '02843_source'))
LAYOUT(DIRECT());

SELECT 1 IN (SELECT dictGet('02843_dict', 'value', materialize('1')));

CREATE TABLE 02843_join (id UInt8, value String) ENGINE Join(ANY, LEFT, id);
SELECT 1 IN (SELECT joinGet(02843_join, 'value', materialize(1)));
SELECT 1 IN (SELECT joinGetOrNull(02843_join, 'value', materialize(1)));

SELECT 1 IN (SELECT materialize(connectionId()));
SELECT 1000000 IN (SELECT materialize(getSetting('max_threads')));
SELECT 1 in (SELECT file(materialize('a'))); -- { serverError FILE_DOESNT_EXIST }

EXPLAIN ESTIMATE SELECT 1 IN (SELECT dictGet('02843_dict', 'value', materialize('1')));
EXPLAIN ESTIMATE  SELECT 1 IN (SELECT joinGet(`02843_join`, 'value', materialize(1)));

DROP DICTIONARY 02843_dict;
DROP TABLE 02843_source;
DROP TABLE 02843_join;
