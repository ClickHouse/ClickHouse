-- `sumForEach` has a trivial destructor but suballocates from the arena on every
-- merge, so the aggregate tree must not use its suffix caches for it (they rebuild
-- their slots by calling create over the previous contents). Check that it still
-- goes through the tree correctly.

SET min_window_frame_rows_for_aggregate_tree = 32;

SELECT
    sum(length(s)),
    sum(arraySum(s)),
    sum(cityHash64(s))
FROM
(
    SELECT sumForEach(arr) OVER (ORDER BY number ROWS BETWEEN 255 PRECEDING AND CURRENT ROW) AS s
    FROM
    (
        SELECT number, arrayMap(x -> (number * x) % 7, range(16)) AS arr
        FROM numbers(20000)
    )
);

-- The same computation without the tree, for the reference output.
SELECT
    sum(length(s)),
    sum(arraySum(s)),
    sum(cityHash64(s))
FROM
(
    SELECT sumForEach(arr) OVER (ORDER BY number ROWS BETWEEN 255 PRECEDING AND CURRENT ROW) AS s
    FROM
    (
        SELECT number, arrayMap(x -> (number * x) % 7, range(16)) AS arr
        FROM numbers(20000)
    )
)
SETTINGS min_window_frame_rows_for_aggregate_tree = 1000000000;
