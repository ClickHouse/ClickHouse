DROP TABLE IF EXISTS mass_table_312;
CREATE TABLE mass_table_312 (d Date DEFAULT '2000-01-01', x UInt64, n Nested(a String, b String)) ENGINE = MergeTree(d, x, 1);
INSERT INTO mass_table_312 SELECT * FROM generateRandom('`d` Date,`x` UInt64,`n.a` Array(String),`n.b` Array(String)', 1, 10, 2) LIMIT 100;

SELECT count(), sum(cityHash64(*)) FROM mass_table_312;
SELECT count(), sum(cityHash64(*)) FROM mass_table_312 ARRAY JOIN n;

DROP TABLE mass_table_312;
