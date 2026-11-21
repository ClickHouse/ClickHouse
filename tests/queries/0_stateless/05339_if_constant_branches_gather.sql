-- `if` and `multiIf` with two constant branches of types without a specialized implementation.
-- 1000 rows make one block, large enough for the gather.

SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toDateTime('2019-02-04 01:24:31', 'UTC'), toDate('2019-02-04')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT multiIf(number % 3, toDateTime('2019-02-04 01:24:31', 'UTC'), toDate('2019-02-04')) AS x FROM numbers(1000)) SETTINGS optimize_multiif_to_if = 1;
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toDate32('1900-01-01'), toDate('2019-02-04')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toDateTime64('2019-02-04 01:24:31.123', 3, 'UTC'), toDateTime64('2019-02-04 01:24:31.123456', 6, 'UTC')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toIPv4('1.2.3.4'), toIPv4('5.6.7.8')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toIPv6('::1'), toIPv6('::ffff:1.2.3.4')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toLowCardinality(toDateTime('2019-02-04 01:24:31', 'UTC')), toLowCardinality(toDate('2019-02-04'))) AS x FROM numbers(1000));

-- Arrays.
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC')], [toDate('2019-02-04')]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT multiIf(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC')], [toDate('2019-02-04')]) AS x FROM numbers(1000)) SETTINGS optimize_multiif_to_if = 1;
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC'), toDateTime('2019-02-05 01:24:31', 'UTC')], [toDate('2019-02-04'), toDate('2019-02-05')]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC'), toDateTime('2019-02-05 01:24:31', 'UTC'), toDateTime('2019-02-06 01:24:31', 'UTC')], [toDate('2019-02-04'), toDate('2019-02-05'), toDate('2019-02-06')]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [], [toDate('2019-02-04')]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toNullable(toDate('2019-02-04')), NULL], [toDateTime('2019-02-04 01:24:31', 'UTC')]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toNullable(toDate('2019-02-04'))], [NULL]) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(arraySum(arrayMap(d -> toUInt32(d), x))) FROM (SELECT if(number % 3, CAST(range(64) AS Array(DateTime('UTC'))), CAST(range(64) AS Array(Date))) AS x FROM numbers(1000));

-- Map and Tuple.
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, map('a', toDate('2019-02-04')), map('b', toDateTime('2019-02-04 01:24:31', 'UTC'))) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, map('a', toDate('2019-02-04')), map('b', toDateTime('2019-02-04 01:24:31', 'UTC'), 'c', toDateTime('2019-02-05 01:24:31', 'UTC'))) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, tuple(toDate('2019-02-04'), 1), tuple(toDateTime('2019-02-04 01:24:31', 'UTC'), 2)) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [CAST(map(), 'Map(String, UInt8)')], CAST([], 'Array(Map(String, UInt16))')) AS x FROM numbers(1000));

-- Variant and Dynamic.
SELECT any(toTypeName(x)), groupArray(7)(variantType(x)), groupArray(7)(x) FROM (SELECT if(number % 3, toDate('2019-02-04'), 1) AS x FROM numbers(1000)) SETTINGS use_variant_as_common_type = 1;
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(number % 3, [toDate('2019-02-04')], [1]) AS x FROM numbers(1000)) SETTINGS use_variant_as_common_type = 1;
SELECT any(toTypeName(x)), groupArray(7)(dynamicType(x)), groupArray(7)(isDynamicElementInSharedData(x)) FROM (SELECT if((number + 1) % 3, CAST(toUInt8(1), 'Dynamic(max_types=1)'), CAST(toUInt16(2), 'Dynamic(max_types=1)')) AS x FROM numbers(1000));

-- AggregateFunction states.
SELECT sumMerge(x) FROM (SELECT if(number % 3, arrayReduce('sumState', [toUInt64(1)]), arrayReduce('sumState', [toUInt64(2)])) AS x FROM numbers(1000));
SELECT groupArray(7)(finalizeAggregation(x)) FROM (SELECT if(number % 3, arrayReduce('sumState', [toUInt64(1)]), arrayReduce('sumState', [toUInt64(2)])) AS x FROM numbers(1000));

-- Nullable condition, several blocks, no rows.
SELECT any(toTypeName(x)), groupArray(7)(x) FROM (SELECT if(materialize(CAST(if(number = 4, NULL, number % 3) AS Nullable(UInt8))), toDateTime('2019-02-04 01:24:31', 'UTC'), toDate('2019-02-04')) AS x FROM numbers(1000));
SELECT any(toTypeName(x)), groupArray(x) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC')], [toDate('2019-02-04')]) AS x FROM numbers(7)) SETTINGS max_block_size = 2;
SELECT any(toTypeName(x)), count(), countIf(x = [toDateTime('2019-02-04 01:24:31', 'UTC')]) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC')], [toDate('2019-02-04')]) AS x FROM numbers(1000)) SETTINGS max_block_size = 300;
SELECT groupArray(x) FROM (SELECT if(number % 3, [toDateTime('2019-02-04 01:24:31', 'UTC')], [toDate('2019-02-04')]) AS x FROM numbers(7) WHERE number > 100);
