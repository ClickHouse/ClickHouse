CREATE TABLE WorksBeforeSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Nested(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY ID;

SET max_static_subcolumns = 2;

CREATE TABLE FailsAfterSubcolumnLimit
(
    `ID` UInt32,
    `Subcolumns` Nested(subcolumn1 UInt32, subcolumn2 UInt32, subcolumn3 DateTime, subcolumn4 Int64, subcolumn5 String, subcolumn6 UInt32)
)
ENGINE = MergeTree
ORDER BY ID; -- { serverError TOO_MANY_SUBCOLUMNS }