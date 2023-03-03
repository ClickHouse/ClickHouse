-- Tags: global

SELECT
    cityHash64(number GLOBAL IN (NULL, -2147483648, -9223372036854775808), nan, 1024, NULL, NULL, 1.000100016593933, NULL),
    (NULL, cityHash64(inf, -2147483648, NULL, NULL, 10.000100135803223), cityHash64(1.1754943508222875e-38, NULL, NULL, NULL), 2147483647)
FROM cluster(test_cluster_two_shards_localhost, numbers((NULL, cityHash64(0., 65536, NULL, NULL, 10000000000., NULL), 0) GLOBAL IN (some_identifier), 65536))
WHERE number GLOBAL IN [1025] --{serverError 284}
