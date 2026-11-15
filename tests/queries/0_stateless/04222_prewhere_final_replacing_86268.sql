-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/86268
-- `PREWHERE` + `FINAL` on `ReplacingMergeTree` with `is_deleted` used to drop the version column and throw `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS repro_86268;

CREATE TABLE repro_86268
(
    id UInt64,
    created_at DateTime,
    is_deleted UInt8
)
ENGINE = ReplacingMergeTree(created_at, is_deleted)
ORDER BY id;

INSERT INTO repro_86268 VALUES (1, '2025-08-25 00:00:00', 0);

SELECT id FROM repro_86268 FINAL PREWHERE created_at <= '2025-08-26' ORDER BY id;

DROP TABLE repro_86268;
