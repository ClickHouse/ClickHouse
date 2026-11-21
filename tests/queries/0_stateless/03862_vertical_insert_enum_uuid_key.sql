-- Test: serialization-sensitive key types (Enum, UUID) sort correctly with vertical insert.
DROP TABLE IF EXISTS t_vi_enum_uuid;

CREATE TABLE t_vi_enum_uuid
(
    k UInt64,
    e Enum8('a' = 1, 'b' = 2),
    u UUID,
    dt64 DateTime64(3),
    fs FixedString(2)
)
ENGINE = MergeTree
ORDER BY (e, u, dt64, fs)
SETTINGS
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    enable_vertical_insert_algorithm = 1,
    vertical_insert_algorithm_min_rows_to_activate = 1,
    vertical_insert_algorithm_min_bytes_to_activate = 0,
    vertical_insert_algorithm_min_columns_to_activate = 1;

INSERT INTO t_vi_enum_uuid VALUES
    (1, 'a', '00000000-0000-0000-0000-000000000001', toDateTime64('2020-01-01 00:00:00.000', 3), 'aa'),
    (2, 'a', '00000000-0000-0000-0000-000000000002', toDateTime64('2020-01-01 00:00:00.000', 3), 'ab'),
    (3, 'b', '00000000-0000-0000-0000-000000000000', toDateTime64('2020-01-01 00:00:00.000', 3), 'bb');

SELECT k, e, u
FROM t_vi_enum_uuid
ORDER BY (e, u, dt64, fs);

DROP TABLE t_vi_enum_uuid;
