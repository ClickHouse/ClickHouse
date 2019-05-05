DROP TABLE IF EXISTS tab_00625;

CREATE TABLE tab_00625
(
    date Date, 
    key UInt32, 
    testMap Nested(
    k UInt16, 
    v UInt64)
)
ENGINE = SummingMergeTree(date, (date, key), 1);

INSERT INTO tab_00625 SELECT 
    today(), 
    number, 
    [toUInt16(number)], 
    [number]
FROM system.numbers 
LIMIT 8190;

INSERT INTO tab_00625 SELECT 
    today(), 
    number + 8190, 
    [toUInt16(number)], 
    [number + 8190]
FROM system.numbers 
LIMIT 10;

OPTIMIZE TABLE tab_00625;
