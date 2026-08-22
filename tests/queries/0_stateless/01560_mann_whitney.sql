DROP TABLE IF EXISTS mann_whitney_test;
CREATE TABLE mann_whitney_test (left Float64, right UInt8) ENGINE = Memory;
INSERT INTO mann_whitney_test VALUES (310, 0), (195, 0), (530, 0), (155, 0), (530, 0), (245, 0), (385, 0), (450, 0), (465, 0), (545, 0), (170, 0), (180, 0), (125, 0), (180, 0), (230, 0), (75, 0), (430, 0), (480, 0), (495, 0), (295, 0), (116, 1), (171, 1), (176, 1), (421, 1), (111, 1), (326, 1), (481, 1), (111, 1), (346, 1), (441, 1), (261, 1), (411, 1), (206, 1), (521, 1), (456, 1), (446, 1), (296, 1), (51, 1), (426, 1), (261, 1);
SELECT mannWhitneyUTest(left, right) from mann_whitney_test;
SELECT '223.0', '0.5426959774289482';
WITH mannWhitneyUTest(left, right) AS pair SELECT roundBankers(pair.1, 16) as t_stat, roundBankers(pair.2, 16) as p_value from mann_whitney_test;
WITH mannWhitneyUTest('two-sided', 1)(left, right) as pair SELECT roundBankers(pair.1, 16) as t_stat, roundBankers(pair.2, 16) as p_value from mann_whitney_test;
WITH mannWhitneyUTest('two-sided')(left, right) as pair SELECT roundBankers(pair.1, 16) as t_stat, roundBankers(pair.2, 16) as p_value from mann_whitney_test;
DROP TABLE IF EXISTS mann_whitney_test;
