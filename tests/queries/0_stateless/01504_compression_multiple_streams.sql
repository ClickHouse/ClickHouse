DROP TABLE IF EXISTS columns_with_multiple_streams;

CREATE TABLE columns_with_multiple_streams (
  field0 Nullable(Int64) CODEC(Delta(2), LZ4),
  field1 Nullable(Int64) CODEC(Delta, LZ4),
  field2 Array(Array(Int64)) CODEC(Delta, LZ4),
  field3 Tuple(UInt32, Array(UInt64)) CODEC(T64, Default)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO columns_with_multiple_streams VALUES(1, 1, [[1]], tuple(1, [1]));

SELECT * FROM columns_with_multiple_streams;

DETACH TABLE columns_with_multiple_streams;
ATTACH TABLE columns_with_multiple_streams;

SELECT * FROM columns_with_multiple_streams;

ALTER TABLE columns_with_multiple_streams MODIFY COLUMN field1 Nullable(UInt8);

INSERT INTO columns_with_multiple_streams VALUES(2, 2, [[2]], tuple(2, [2]));

SHOW CREATE TABLE columns_with_multiple_streams;

SELECT * FROM columns_with_multiple_streams ORDER BY field0;

ALTER TABLE columns_with_multiple_streams MODIFY COLUMN field3 CODEC(Delta, Default);

SHOW CREATE TABLE columns_with_multiple_streams;

INSERT INTO columns_with_multiple_streams VALUES(3, 3, [[3]], tuple(3, [3]));

SELECT * FROM columns_with_multiple_streams ORDER BY field0;

DROP TABLE IF EXISTS columns_with_multiple_streams;

