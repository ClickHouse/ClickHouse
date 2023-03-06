DROP TABLE IF EXISTS json_columns;

CREATE TABLE json_columns (n UInt32, s String) ENGINE = MergeTree order by n;

SELECT * FROM json_columns FORMAT JSONColumns;
