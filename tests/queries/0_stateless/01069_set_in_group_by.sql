DROP TABLE IF EXISTS testmt;

CREATE TABLE testmt (`CounterID` UInt64, `value` String) ENGINE = MergeTree() ORDER BY CounterID;

INSERT INTO testmt VALUES (1, '1'), (2, '2');

SELECT arrayJoin([CounterID NOT IN (2)]) AS counter FROM testmt WHERE CounterID IN (2) GROUP BY counter;

DROP TABLE testmt;
