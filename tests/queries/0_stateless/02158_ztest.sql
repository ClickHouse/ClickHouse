DROP TABLE IF EXISTS mean_ztest;
CREATE TABLE mean_ztest (v int, s UInt8) ENGINE = Memory;
INSERT INTO mean_ztest SELECT number, 0 FROM numbers(100) WHERE number % 2 = 0;
INSERT INTO mean_ztest SELECT number, 1 FROM numbers(100) WHERE number % 2 = 1;
SELECT roundBankers(meanZTest(833.0, 800.0, 0.95)(v, s).1, 16) as z_stat, roundBankers(meanZTest(833.0, 800.0, 0.95)(v, s).2, 16) as p_value, roundBankers(meanZTest(833.0, 800.0, 0.95)(v, s).3, 16) as ci_low, roundBankers(meanZTest(833.0, 800.0, 0.95)(v, s).4, 16) as ci_high FROM mean_ztest;
DROP TABLE IF EXISTS mean_ztest;
