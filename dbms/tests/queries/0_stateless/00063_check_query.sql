DROP TABLE IF EXISTS check_query_tiny_log;

CREATE TABLE check_query_tiny_log (N UInt32, S String) Engine = TinyLog;

INSERT INTO check_query_tiny_log VALUES (1, 'A'), (2, 'B'), (3, 'C')

DROP TABLE IF EXISTS check_query_log;

CREATE TABLE check_query_log (N UInt32,S String) Engine = Log;

INSERT INTO check_query_log VALUES (1, 'A'), (2, 'B'), (3, 'C')
