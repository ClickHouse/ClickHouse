DROP TABLE IF EXISTS simple_agf_aggregating_mt;

CREATE TABLE simple_agf_aggregating_mt
(
    a Int64,
    min_aggreg AggregateFunction(min, UInt64),
    min_simple SimpleAggregateFunction(min, UInt64),
    max_aggreg AggregateFunction(max, UInt64),
    max_simple SimpleAggregateFunction(max, UInt64),
    sum_aggreg AggregateFunction(sum, UInt64),
    sum_simple SimpleAggregateFunction(sum, UInt64),
    sumov_aggreg AggregateFunction(sumWithOverflow, UInt64),
    sumov_simple SimpleAggregateFunction(sumWithOverflow, UInt64),
    gbitand_aggreg AggregateFunction(groupBitAnd, UInt64),
    gbitand_simple SimpleAggregateFunction(groupBitAnd, UInt64),
    gbitor_aggreg AggregateFunction(groupBitOr, UInt64),
    gbitor_simple SimpleAggregateFunction(groupBitOr, UInt64),
    gbitxor_aggreg AggregateFunction(groupBitXor, UInt64),
    gbitxor_simple SimpleAggregateFunction(groupBitXor, UInt64),
    gra_aggreg AggregateFunction(groupArrayArray, Array(UInt64)),
    gra_simple SimpleAggregateFunction(groupArrayArray, Array(UInt64)),
    grp_aggreg AggregateFunction(groupUniqArrayArray, Array(UInt64)),
    grp_simple SimpleAggregateFunction(groupUniqArrayArray, Array(UInt64)),
    aggreg_map AggregateFunction(sumMap, Tuple(Array(String), Array(UInt64))),
    simple_map SimpleAggregateFunction(sumMap, Tuple(Array(String), Array(UInt64))),
    aggreg_map_min AggregateFunction(minMap, Tuple(Array(String), Array(UInt64))),
    simple_map_min SimpleAggregateFunction(minMap, Tuple(Array(String), Array(UInt64))),
    aggreg_map_max AggregateFunction(maxMap, Tuple(Array(String), Array(UInt64))),
    simple_map_max SimpleAggregateFunction(maxMap, Tuple(Array(String), Array(UInt64)))
)
ENGINE = AggregatingMergeTree
ORDER BY a;

INSERT INTO simple_agf_aggregating_mt SELECT
    number % 51 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState(number + 111111111),
    groupBitAnd(number + 111111111),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([toUInt64(number % 1000)]),
    groupArrayArray([toUInt64(number % 1000)]),
    groupUniqArrayArrayState([toUInt64(number % 500)]),
    groupUniqArrayArray([toUInt64(number % 500)]),
    sumMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    sumMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

INSERT INTO simple_agf_aggregating_mt SELECT
    number % 1151 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState(number + 111111111),
    groupBitAnd(number + 111111111),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([toUInt64(number % 1000)]),
    groupArrayArray([toUInt64(number % 1000)]),
    groupUniqArrayArrayState([toUInt64(number % 500)]),
    groupUniqArrayArray([toUInt64(number % 500)]),
    sumMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    sumMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

OPTIMIZE TABLE simple_agf_aggregating_mt FINAL;

SELECT cityHash64(groupArray(cityHash64(*))) FROM (
  SELECT
    a % 31 AS g,
    minMerge(min_aggreg) AS minagg,
    min(min_simple) AS mins,
    minagg = mins AS M,
    maxMerge(max_aggreg) AS maxagg,
    max(max_simple) AS maxs,
    maxagg = maxs AS MX,
    sumMerge(sum_aggreg) AS sumagg,
    sum(sum_simple) AS sums,
    sumagg = sums AS S,
    sumWithOverflowMerge(sumov_aggreg) AS sumaggov,
    sumWithOverflow(sumov_simple) AS sumsov,
    sumaggov = sumsov AS SO,
    groupBitAndMerge(gbitand_aggreg) AS gbitandaggreg,
    groupBitAnd(gbitand_simple) AS gbitandsimple,
    gbitandaggreg = gbitandsimple AS BIT_AND,
    groupBitOrMerge(gbitor_aggreg) AS gbitoraggreg,
    groupBitOr(gbitor_simple) AS gbitorsimple,
    gbitoraggreg = gbitorsimple AS BIT_OR,
    groupBitXorMerge(gbitxor_aggreg) AS gbitxoraggreg,
    groupBitXor(gbitxor_simple) AS gbitxorsimple,
    gbitxoraggreg = gbitxorsimple AS BITXOR,
    arraySort(groupArrayArrayMerge(gra_aggreg)) AS graa,
    arraySort(groupArrayArray(gra_simple)) AS gras,
    graa = gras AS GAA,
    arraySort(groupUniqArrayArrayMerge(grp_aggreg)) AS gra,
    arraySort(groupUniqArrayArray(grp_simple)) AS grs,
    gra = grs AS T,
    sumMapMerge(aggreg_map) AS smmapagg,
    sumMap(simple_map) AS smmaps,
    smmapagg = smmaps AS SM,
    minMapMerge(aggreg_map_min) AS minmapapagg,
    minMap(simple_map_min) AS minmaps,
    minmapapagg = minmaps AS SMIN,
    maxMapMerge(aggreg_map_max) AS maxmapapagg,
    maxMap(simple_map_max) AS maxmaps,
    maxmapapagg = maxmaps AS SMAX
  FROM simple_agf_aggregating_mt
  GROUP BY g
  ORDER BY g
);

SELECT '---mutation---';

ALTER TABLE simple_agf_aggregating_mt
    DELETE WHERE (a % 3) = 0
SETTINGS mutations_sync = 1;

INSERT INTO simple_agf_aggregating_mt SELECT
    number % 11151 AS a,
    minState(number),
    min(number),
    maxState(number),
    max(number),
    sumState(number),
    sum(number),
    sumWithOverflowState(number),
    sumWithOverflow(number),
    groupBitAndState((number % 3) + 111111110),
    groupBitAnd((number % 3) + 111111110),
    groupBitOrState(number + 111111111),
    groupBitOr(number + 111111111),
    groupBitXorState(number + 111111111),
    groupBitXor(number + 111111111),
    groupArrayArrayState([toUInt64(number % 100)]),
    groupArrayArray([toUInt64(number % 100)]),
    groupUniqArrayArrayState([toUInt64(number % 50)]),
    groupUniqArrayArray([toUInt64(number % 50)]),
    sumMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    sumMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    minMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMapState((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13)))),
    maxMap((arrayMap(i -> toString(i), range(13)), arrayMap(i -> (number + i), range(13))))
FROM numbers(10000)
GROUP BY a;

OPTIMIZE TABLE simple_agf_aggregating_mt FINAL;

SELECT cityHash64(groupArray(cityHash64(*))) FROM (
SELECT
    a % 31 AS g,
    minMerge(min_aggreg) AS minagg,
    min(min_simple) AS mins,
    minagg = mins AS M,
    maxMerge(max_aggreg) AS maxagg,
    max(max_simple) AS maxs,
    maxagg = maxs AS MX,
    sumMerge(sum_aggreg) AS sumagg,
    sum(sum_simple) AS sums,
    sumagg = sums AS S,
    sumWithOverflowMerge(sumov_aggreg) AS sumaggov,
    sumWithOverflow(sumov_simple) AS sumsov,
    sumaggov = sumsov AS SO,
    groupBitAndMerge(gbitand_aggreg) AS gbitandaggreg,
    groupBitAnd(gbitand_simple) AS gbitandsimple,
    gbitandaggreg = gbitandsimple AS BIT_AND,
    groupBitOrMerge(gbitor_aggreg) AS gbitoraggreg,
    groupBitOr(gbitor_simple) AS gbitorsimple,
    gbitoraggreg = gbitorsimple AS BIT_OR,
    groupBitXorMerge(gbitxor_aggreg) AS gbitxoraggreg,
    groupBitXor(gbitxor_simple) AS gbitxorsimple,
    gbitxoraggreg = gbitxorsimple AS BITXOR,
    arraySort(groupArrayArrayMerge(gra_aggreg)) AS graa,
    arraySort(groupArrayArray(gra_simple)) AS gras,
    graa = gras AS GAA,
    arraySort(groupUniqArrayArrayMerge(grp_aggreg)) AS gra,
    arraySort(groupUniqArrayArray(grp_simple)) AS grs,
    gra = grs AS T,
    sumMapMerge(aggreg_map) AS smmapagg,
    sumMap(simple_map) AS smmaps,
    smmapagg = smmaps AS SM,
    minMapMerge(aggreg_map_min) AS minmapapagg,
    minMap(simple_map_min) AS minmaps,
    minmapapagg = minmaps AS SMIN,
    maxMapMerge(aggreg_map_max) AS maxmapapagg,
    maxMap(simple_map_max) AS maxmaps,
    maxmapapagg = maxmaps AS SMAX
  FROM simple_agf_aggregating_mt
  GROUP BY g
  ORDER BY g
);

DROP TABLE simple_agf_aggregating_mt;
