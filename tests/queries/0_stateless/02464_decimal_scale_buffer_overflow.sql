DROP TABLE IF EXISTS series__fuzz_35;
CREATE TABLE series__fuzz_35 (`i` UInt8, `x_value` Decimal(18, 14), `y_value` DateTime) ENGINE = Memory;
INSERT INTO series__fuzz_35(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
SELECT skewSamp(x_value) FROM (SELECT x_value as x_value FROM series__fuzz_35 LIMIT 2) FORMAT Null;
DROP TABLE series__fuzz_35;
