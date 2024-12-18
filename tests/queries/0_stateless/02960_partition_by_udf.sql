-- Tags: no-parallel

DROP FUNCTION IF EXISTS f1;
CREATE FUNCTION f1 AS (x) -> x;

CREATE TABLE hit
(
  `UserID` UInt32,
  `URL` String,
  `EventTime` DateTime
)
ENGINE = MergeTree
partition by f1(URL)
ORDER BY (EventTime);

INSERT INTO hit SELECT * FROM generateRandom() LIMIT 10;
SELECT count() FROM hit;

DROP TABLE hit;
