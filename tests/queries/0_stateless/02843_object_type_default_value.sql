DROP TABLE IF EXISTS ingest;
SET allow_experimental_object_type = 1;
CREATE TABLE ingest
(
      appname String,
      datetime DateTime,
      app1 Object('json'),
      app2 Object('json')
)
ENGINE = MergeTree()
ORDER BY (
   appname,
   datetime
);

INSERT INTO ingest (appname, datetime, app1) VALUES ('app1', '2023-08-17 10:10:43', '{"key": "value"}');

SELECT * FROM ingest;

DROP TABLE ingest;
