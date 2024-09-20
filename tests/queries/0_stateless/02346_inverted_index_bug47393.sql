SET allow_experimental_inverted_index = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx str TYPE inverted(3) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1;

INSERT INTO tab (str) VALUES ('I am inverted');

SELECT data_version FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active = 1;

-- update column synchronously
ALTER TABLE tab UPDATE str = 'I am not inverted' WHERE 1 SETTINGS mutations_sync=1;

SELECT data_version FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active = 1;

SELECT str FROM tab WHERE str LIKE '%inverted%' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tab;
