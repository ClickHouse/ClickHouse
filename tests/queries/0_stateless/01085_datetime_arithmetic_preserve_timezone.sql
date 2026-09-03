SELECT toDateTime('2020-01-01 00:00:00', 'UTC') AS t, t + 1, toDate(t) + 1, t + INTERVAL 1 SECOND, t + INTERVAL 1 DAY, toTypeName(t + 1), toDateTime64(t, 3, 'UTC') + 1 AS dt64, toTypeName(dt64);
