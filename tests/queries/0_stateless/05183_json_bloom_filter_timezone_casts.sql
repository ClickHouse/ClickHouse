DROP TABLE IF EXISTS json_bf_timezone;
CREATE TABLE json_bf_timezone (id UInt64, j JSON, INDEX bf j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- Preserve the UTC runtime types independently of the session timezone.
INSERT INTO json_bf_timezone
SELECT * FROM format(RowBinary, 'id UInt64, j JSON', concat(
    formatRowNoNewline('RowBinary', toUInt64(1)), unhex('02026474'),
    formatRowNoNewline('RowBinary', CAST(toDateTime('2026-01-01 00:00:00', 'UTC') AS Dynamic)), unhex('0464743634'),
    formatRowNoNewline('RowBinary', CAST(toDateTime64('2026-01-01 00:00:00.123', 3, 'UTC') AS Dynamic)),
    formatRowNoNewline('RowBinary', toUInt64(2)), unhex('02026474'),
    formatRowNoNewline('RowBinary', CAST(toDateTime('2026-01-02 00:00:00', 'UTC') AS Dynamic)), unhex('0464743634'),
    formatRowNoNewline('RowBinary', CAST(toDateTime64('2026-01-02 00:00:00.123', 3, 'UTC') AS Dynamic))))
SETTINGS input_format_binary_read_json_as_string = 0;

SELECT arraySort(groupArray(id)) FROM json_bf_timezone WHERE CAST(j.dt AS DateTime('Europe/Berlin')) = '2026-01-01 01:00:00' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_timezone WHERE CAST(j.dt AS DateTime('Europe/Berlin')) = '2026-01-01 01:00:00' SETTINGS force_data_skipping_indices = 'bf';
SELECT arraySort(groupArray(id)) FROM json_bf_timezone WHERE CAST(j.dt64 AS DateTime64(3, 'Europe/Berlin')) = '2026-01-01 01:00:00.123' SETTINGS use_skip_indexes = 0;
SELECT arraySort(groupArray(id)) FROM json_bf_timezone WHERE CAST(j.dt64 AS DateTime64(3, 'Europe/Berlin')) = '2026-01-01 01:00:00.123' SETTINGS force_data_skipping_indices = 'bf';

DROP TABLE json_bf_timezone;
