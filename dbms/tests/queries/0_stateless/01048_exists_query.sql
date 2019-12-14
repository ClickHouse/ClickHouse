EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;

DROP DATABASE IF EXISTS database_for_dict;
CREATE DATABASE database_for_dict Engine = Ordinary;

DROP TABLE IF EXISTS database_for_dict.t;
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;

CREATE TABLE database_for_dict.t (x UInt8) ENGINE = Memory;
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;

DROP TABLE database_for_dict.t;
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;

DROP DICTIONARY IF EXISTS t;
CREATE TEMPORARY TABLE t (x UInt8);
EXISTS t; -- Does not work for temporary tables. Maybe have to fix.
EXISTS TABLE t;
EXISTS DICTIONARY t;

CREATE DICTIONARY database_for_dict.t (k UInt64, v String) PRIMARY KEY k LAYOUT(FLAT()) SOURCE(HTTP(URL 'http://example.test/' FORMAT TSV)) LIFETIME(1000);
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t; -- Dictionaries are tables as well. But not all tables are dictionaries.
EXISTS DICTIONARY database_for_dict.t;

-- But dictionary-tables cannot be dropped as usual tables.
DROP TABLE database_for_dict.t; -- { serverError 60 }
DROP DICTIONARY database_for_dict.t;
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;

DROP DATABASE database_for_dict;
EXISTS database_for_dict.t;
EXISTS TABLE database_for_dict.t;
EXISTS DICTIONARY database_for_dict.t;
