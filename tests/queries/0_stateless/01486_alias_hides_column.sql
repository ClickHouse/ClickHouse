DROP TABLE IF EXISTS currencyRateShard;
DROP TABLE IF EXISTS currencyRate;

CREATE TABLE currencyRateShard (`date` Date, `base` FixedString(3)) ENGINE = MergeTree ORDER BY (date);
CREATE TABLE currencyRate AS currencyRateShard ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), currencyRateShard);

INSERT INTO currencyRateShard VALUES (0, 'aaa');
INSERT INTO currencyRateShard VALUES (0, 'bbb');

SELECT base, MAX(`date`) AS `date` FROM currencyRateShard GROUP BY base ORDER BY base;

DROP TABLE IF EXISTS currencyRateShard;
DROP TABLE IF EXISTS currencyRate;
