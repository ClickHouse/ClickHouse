SELECT cityHash64(map(1, 'Hello'), CAST(materialize('World') AS LowCardinality(String)));
SELECT cityHash64(map(), CAST(materialize('') AS LowCardinality(Nullable(String))));
SELECT materialize(42) as last_element, cityHash64(map(), CAST(materialize('') AS LowCardinality(Nullable(String))), last_element) from numbers(3);

SET allow_suspicious_low_cardinality_types = 1;
CREATE TEMPORARY TABLE datetime__fuzz_14 (`d` LowCardinality(Nullable(UInt128)));
SELECT max(mapPopulateSeries(mapPopulateSeries(map(toInt64(1048), toInt64(9223), 3, -2147))), toInt64(1048), map('11', 257, '', NULL), cityHash64(*)) > NULL FROM (SELECT max(cityHash64(mapPopulateSeries(mapPopulateSeries(map(toInt64(1048), toInt64(2147), 655, -2147))), *)) > NULL, map(toInt64(-2147), toInt64(100.0001), -2147, NULL), mapPopulateSeries(map(toInt64(1024), toInt64(1048), 1048, -1)), map(toInt64(256), toInt64(NULL), -1, NULL), quantile(0.0001)(d) FROM datetime__fuzz_14 WITH TOTALS);
