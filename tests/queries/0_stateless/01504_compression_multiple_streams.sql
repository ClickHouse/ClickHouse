DROP TABLE IF EXISTS columns_with_multiple_streams;

CREATE TABLE columns_with_multiple_streams (
  field0 Nullable(Int64) CODEC(Delta(2), LZ4),
  field1 Nullable(Int64) CODEC(Delta, LZ4),
  field2 Array(Array(Int64)) CODEC(Delta, LZ4),
  filed3 Tuple(UInt32, Array(UInt64)) CODEC(T64, Default)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO columns_with_multiple_streams VALUES(1, 1, [[1]], tuple(1, [1]));

SELECT * FROM columns_with_multiple_streams;

DROP TABLE IF EXISTS columns_with_multiple_streams;

