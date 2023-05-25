DROP TABLE IF EXISTS testView;
DROP TABLE IF EXISTS testTable;

CREATE TABLE IF NOT EXISTS testTable (
 A LowCardinality(String), -- like voter
 B Int64
) ENGINE MergeTree()
ORDER BY (A);

INSERT INTO testTable VALUES ('A', 1),('B',2),('C',3);

CREATE VIEW testView AS 
SELECT
 A as ALow, -- like account
 B
FROM
   testTable;

SELECT CAST(ALow, 'String') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

DROP TABLE testTable;

CREATE TABLE IF NOT EXISTS testTable (
 A String, -- like voter
 B Int64
) ENGINE MergeTree()
ORDER BY (A);

SELECT CAST(ALow, 'String') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

INSERT INTO testTable VALUES ('A', 1),('B',2),('C',3);

SELECT CAST(ALow, 'String') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

DROP TABLE IF EXISTS testView;
DROP TABLE IF EXISTS testTable;
