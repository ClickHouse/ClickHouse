DROP TABLE IF EXISTS t_map_int_key;
CREATE TABLE t_map_int_key (m1 Map(UInt32, UInt32), m2 Map(Date, UInt32)) ENGINE = Memory;

INSERT INTO t_map_int_key FORMAT CSV "{1:2, 3: 4, 5 :6, 7 : 8}","{'2021-05-20':1, '2021-05-21': 2, '2021-05-22' :3, '2021-05-23' : 4}"

SELECT m1, m2 FROM t_map_int_key;

DROP TABLE t_map_int_key;
