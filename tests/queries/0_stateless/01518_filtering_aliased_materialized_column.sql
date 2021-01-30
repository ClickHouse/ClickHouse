DROP TABLE IF EXISTS logs;

CREATE TABLE logs( 
  date_visited DateTime, 
  date Date MATERIALIZED toDate(date_visited)
) ENGINE = MergeTree() ORDER BY tuple();

SELECT count() FROM logs AS plogs WHERE plogs.date = '2019-11-20';

INSERT INTO logs VALUES('2019-11-20 00:00:00');

SELECT count() FROM logs AS plogs WHERE plogs.date = '2019-11-20';

DROP TABLE logs;
