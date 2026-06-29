-- https://github.com/ClickHouse/ClickHouse/issues/57193
SELECT
    2147483647,
    count(pow(NULL, 1.0001))
FROM remote(test_cluster_two_shards, system, one)
GROUP BY
    makeDateTime64(NULL, NULL, pow(NULL, '257') - '-1', '0.2147483647', 257),
    makeDateTime64(pow(pow(NULL, '21474836.46') - '0.0000065535', 1048577), '922337203685477580.6', NULL, NULL, pow(NULL, 1.0001) - 65536, NULL)
WITH CUBE
    SETTINGS enable_analyzer = 1;


CREATE TABLE data_01223 (`key` Int) ENGINE = Memory;
CREATE TABLE dist_layer_01223 AS data_01223 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_01223);
CREATE TABLE dist_01223 AS data_01223 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01223);
SELECT count(round('92233720368547758.07', '-0.01', NULL, nan, '25.7', '-92233720368547758.07', NULL))
FROM dist_01223
WHERE round(NULL, 1025, 1.1754943508222875e-38, NULL)
WITH TOTALS
    SETTINGS enable_analyzer = 1;
