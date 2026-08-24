DROP TABLE IF EXISTS tutorial;
create table tutorial ( inner_poly  Array(Tuple(Int32, Int32)), outer_poly  Array(Tuple(Int32, Int32)) ) engine = Log();

SELECT * FROM tutorial;

INSERT INTO tutorial VALUES ([(123, 456), (789, 234)], [(567, 890)]), ([], [(11, 22), (33, 44), (55, 66)]);
SELECT * FROM tutorial;

DROP TABLE tutorial;
