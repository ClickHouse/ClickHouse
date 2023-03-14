DROP TABLE IF EXISTS 02680_datetime64_monotonic_check;

CREATE TABLE 02680_datetime64_monotonic_check (`t` DateTime64(3), `x` Nullable(Decimal(18, 14)))
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(t)
ORDER BY x SETTINGS allow_nullable_key = 1;

INSERT INTO 02680_datetime64_monotonic_check VALUES (toDateTime64('2023-03-13 00:00:00', 3, 'Asia/Jerusalem'), 123);

SELECT toHour(toTimeZone(t, 'UTC')) AS toHour_UTC, toHour(toTimeZone(t, 'Asia/Jerusalem')) AS toHour_Israel, count()
FROM 02680_datetime64_monotonic_check
WHERE toHour_Israel = 0
GROUP BY toHour_UTC, toHour_Israel;

DROP TABLE 02680_datetime64_monotonic_check;
