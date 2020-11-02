DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS distributed_data;

CREATE TABLE IF NOT EXISTS data
(
    event_date Date,
    ip_src     UInt32,
    ip_dst     UInt32
) Engine = MergeTree()
    PARTITION BY toYYYYMM(event_date)
    ORDER BY (intHash32(ip_src))
    PRIMARY KEY (intHash32(ip_src))
    SAMPLE BY intHash32(ip_src);

CREATE TABLE IF NOT EXISTS distributed_data
(
    event_date Date,
    ip_src     UInt32,
    ip_dst     UInt32
) Engine = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data);

INSERT INTO data SELECT '2000-01-01', number, number FROM system.numbers WHERE intHash32(number) < 0xFFFFFFFF / 10 LIMIT 10;

SELECT DISTINCT ip_src as ip
FROM distributed_data SAMPLE 1 / 10 OFFSET 0 / 10
WHERE event_date = '2000-01-01';

DROP TABLE data;
DROP TABLE distributed_data;
