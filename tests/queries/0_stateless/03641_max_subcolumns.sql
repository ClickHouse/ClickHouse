CREATE TABLE WorksBeforeSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Nested(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn3 Int64, subcolumn4 String, subcolumn5 UInt32)
)
ENGINE = MergeTree
ORDER BY CounterID

SET max_subcolumns = 2;

CREATE TABLE FailsAfterSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Nested(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY CounterID