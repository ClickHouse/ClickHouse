-- Disable force_primary_key_reverse_order: tests ALTER TABLE operations, output order depends on key direction
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS nullable_00571;
CREATE TABLE nullable_00571 (x String) ENGINE = MergeTree ORDER BY x;
INSERT INTO nullable_00571 VALUES ('hello'), ('world');
SELECT * FROM nullable_00571;
ALTER TABLE nullable_00571 ADD COLUMN n Nullable(UInt64);
SELECT * FROM nullable_00571;
ALTER TABLE nullable_00571 DROP COLUMN n;
ALTER TABLE nullable_00571 ADD COLUMN n Nullable(UInt64) DEFAULT NULL;
SELECT * FROM nullable_00571;
ALTER TABLE nullable_00571 DROP COLUMN n;
ALTER TABLE nullable_00571 ADD COLUMN n Nullable(UInt64) DEFAULT 0;
SELECT * FROM nullable_00571;
DROP TABLE nullable_00571;
