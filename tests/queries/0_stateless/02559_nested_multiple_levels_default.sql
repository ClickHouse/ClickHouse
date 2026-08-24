DROP TABLE IF EXISTS data_compact;
DROP TABLE IF EXISTS data_memory;
DROP TABLE IF EXISTS data_wide;

-- compact
DROP TABLE IF EXISTS data_compact;
CREATE TABLE data_compact
(
    `root.array` Array(UInt8),
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_rows_for_wide_part=100, min_bytes_for_wide_part=1e9;
INSERT INTO data_compact VALUES ([0]);
ALTER TABLE data_compact ADD COLUMN root.nested_array Array(Array(UInt8));
SELECT table, part_type FROM system.parts WHERE table = 'data_compact' AND database = currentDatabase();
SELECT root.nested_array FROM data_compact;

-- wide
DROP TABLE IF EXISTS data_wide;
CREATE TABLE data_wide
(
    `root.array` Array(UInt8),
)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0;
INSERT INTO data_wide VALUES ([0]);
ALTER TABLE data_wide ADD COLUMN root.nested_array Array(Array(UInt8));
SELECT table, part_type FROM system.parts WHERE table = 'data_wide' AND database = currentDatabase();
SELECT root.nested_array FROM data_wide;
