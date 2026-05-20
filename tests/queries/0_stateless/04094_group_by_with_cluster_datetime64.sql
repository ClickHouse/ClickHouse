-- `DateTime64` / `Time64` store ticks at the type's scale in a backing
-- `Int64` and inherit the wide-integer precision problem. They use the same
-- translate-by-min path as `Int64` so bucket arithmetic stays exact, and
-- `<distance>` is interpreted in the native tick unit (microseconds for
-- `scale = 6`, nanoseconds for `scale = 9`, ...).

-- 3 consecutive microsecond-precision timestamps differ by 1 tick (1 µs),
-- merge into a single cluster at `WITH CLUSTER 1`.
SELECT count() AS num_clusters, sum(c) AS total_rows
FROM (
    SELECT ts, count() AS c
    FROM (
        SELECT toDateTime64('2024-01-01 00:00:00', 6) + INTERVAL number MICROSECOND AS ts
        FROM numbers(3)
    )
    GROUP BY ts WITH CLUSTER 1
);

-- Same data, distance one second below the chain threshold: the three rows
-- stay separate. `WITH CLUSTER` distance is in ticks, so `1000000` µs == 1 s,
-- and a `999999` µs threshold leaves the second-apart points apart.
SELECT count() AS num_clusters
FROM (
    SELECT ts, count() AS c
    FROM (
        SELECT toDateTime64('2024-01-01 00:00:00', 6) + INTERVAL number SECOND AS ts
        FROM numbers(3)
    )
    GROUP BY ts WITH CLUSTER 999999
);

-- Same data, one-second threshold in native ticks: chain-merges all three.
SELECT count() AS num_clusters
FROM (
    SELECT ts, count() AS c
    FROM (
        SELECT toDateTime64('2024-01-01 00:00:00', 6) + INTERVAL number SECOND AS ts
        FROM numbers(3)
    )
    GROUP BY ts WITH CLUSTER 1000000
);
