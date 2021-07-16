CREATE TABLE tmp ENGINE = TinyLog AS SELECT queryID();
SYSTEM FLUSH LOGS;
SELECT query FROM system.query_log WHERE query_id = (SELECT * FROM tmp) LIMIT 1;
DROP TABLE tmp;

CREATE TABLE tmp ENGINE = TinyLog AS SELECT initialQueryID();
SYSTEM FLUSH LOGS;
SELECT query FROM system.query_log WHERE initial_query_id = (SELECT * FROM tmp) LIMIT 1;
DROP TABLE tmp;

