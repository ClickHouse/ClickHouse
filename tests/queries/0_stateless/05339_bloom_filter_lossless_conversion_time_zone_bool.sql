-- A skip index is not analyzed through a cast to another time zone or from `UInt8` to `Bool`. These types are
-- equal by `IDataType::equals`, but the time zone changes how a literal is parsed, and `Bool` converts 2 to 1.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    dt DateTime('Asia/Tokyo'),
    dt_utc DateTime('UTC') ALIAS dt,
    u UInt8,
    b Bool ALIAS u,
    INDEX idx_dt dt TYPE bloom_filter GRANULARITY 1,
    INDEX idx_u u TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO tab (id, dt, u) VALUES
    (1, toDateTime('2024-01-01 00:00:00', 'UTC'), 0),
    (2, toDateTime('2024-01-01 00:00:00', 'Asia/Tokyo'), 1),
    (3, toDateTime('2024-01-02 00:00:00', 'UTC'), 2);

SELECT '-- time zone';
SELECT id FROM tab WHERE dt_utc = '2024-01-01 00:00:00' ORDER BY id;
SELECT id FROM tab WHERE CAST(dt, 'DateTime(\'UTC\')') = '2024-01-01 00:00:00' ORDER BY id;
SELECT id FROM tab WHERE CAST(dt, 'DateTime(\'UTC\')') IN ('2024-01-01 00:00:00', '2024-01-02 00:00:00') ORDER BY id;

SELECT '-- Bool';
SELECT id FROM tab WHERE b = true ORDER BY id;
SELECT id FROM tab WHERE CAST(u, 'Bool') IN (true) ORDER BY id;

SELECT '-- the same time zone is still looked through';
SELECT id FROM tab WHERE CAST(dt, 'Nullable(DateTime(\'Asia/Tokyo\'))') = '2024-01-01 00:00:00' ORDER BY id SETTINGS force_data_skipping_indices = 'idx_dt';

DROP TABLE tab;
