-- The input is table(test text, query text, run UInt32, version UInt8, metrics Array(float)).
-- Randomization test for the median difference between the two versions.
--
-- Two optimizations over the naive cross-join approach:
--  1) Keep each query's measurements as one array and reshuffle per iteration
--     (bounded by max_block_size), instead of materializing/sorting numbers*T rows.
--  2) Only randomize metrics that actually vary across the measurements.
--     A metric that is constant has identical medians in every random split, so
--     its threshold is exactly 0; computing it is pure waste (most of the ~230
--     ProfileEvents are constant per query). The threshold is scattered back to
--     full width with 0 for the constant metrics.
WITH
    (SELECT groupArray(metrics) FROM table) AS all_metrics,
    length(all_metrics[1]) AS num_metrics,
    arrayFilter(i -> arrayMin(arrayMap(row -> row[i], all_metrics))
                     != arrayMax(arrayMap(row -> row[i], all_metrics)),
                range(1, num_metrics + 1)) AS varying,
    arrayMap(row -> arrayMap(i -> row[i], varying), all_metrics) AS projected
SELECT
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[1] as l) l_rounded,
    arrayMap(x -> floor(x, 4), original_medians_array.medians_by_version[2] as r) r_rounded,
    arrayMap(x, y -> floor((y - x) / x, 3), l, r) diff_percent,
    arrayMap(x, y -> floor(x / y, 3), threshold, l) threshold_percent,
    test, query
from
    (
        -- randomization over the varying metrics only, then scattered back to
        -- full width (constant metrics get threshold 0)
        select arrayMap(i -> if(has(varying, i), thr_small[indexOf(varying, i)], 0.),
                        range(1, num_metrics + 1)) threshold
        from
        (
            select quantileExactForEach(0.99)(d) thr_small
            from
            (
                select
                    arrayShuffle(projected) as sh,
                    intDiv(length(sh), 2) as h,
                    arrayReduce('medianExactForEach', arraySlice(sh, 1, h)) as ma,
                    arrayReduce('medianExactForEach', arraySlice(sh, h + 1)) as mb,
                    arrayMap(x, y -> abs(x - y), ma, mb) as d
                from numbers(10000)
                settings max_block_size = 64
            )
        )
    ) rd,
    (
        select groupArrayInsertAt(median_metrics, version) medians_by_version
        from
        (
            select medianExactForEach(metrics) median_metrics, version
            from table
            group by version
        ) original_medians
    ) original_medians_array,
    (
        select any(test) test, any(query) query from table
    ) any_query,
    (
       select throwIf(uniq((test, query)) != 1) from table
    ) check_single_query
;
