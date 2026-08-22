SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS map_comb;
CREATE TABLE map_comb(a int, statusMap Map(UInt16, UInt32)) ENGINE = Log;

INSERT INTO map_comb VALUES (1, map(1, 10, 2, 10, 3, 10)),(1, map(3, 10, 4, 10, 5, 10)),(2, map(4, 10, 5, 10, 6, 10)),(2, map(6, 10, 7, 10, 8, 10)),(3, map(1, 10, 2, 10, 3, 10)),(4, map(3, 10, 4, 10, 5, 10)),(5, map(4, 10, 5, 10, 6, 10)),(5, map(6, 10, 7, 10, 8, 10));

SELECT * FROM map_comb ORDER BY a, statusMap;
SELECT toTypeName(res), sumMap(statusMap) as res FROM map_comb;
SELECT toTypeName(res), sumWithOverflowMap(statusMap) as res FROM map_comb;
SELECT toTypeName(res), sumMapMerge(s) as res FROM (SELECT sumMapState(statusMap) AS s FROM map_comb);
SELECT minMap(statusMap) FROM map_comb;
SELECT maxMap(statusMap) FROM map_comb;
SELECT toTypeName(res), avgMap(statusMap) as res FROM map_comb;
SELECT countMap(statusMap) FROM map_comb;
SELECT a, sumMap(statusMap) FROM map_comb GROUP BY a ORDER BY a;

DROP TABLE map_comb;

-- check different types
select minMap(val) from values ('val Map(UUID, Int32)',
	(map('01234567-89ab-cdef-0123-456789abcdef', 1)),
	(map('01234567-89ab-cdef-0123-456789abcdef', 2)));
select minMap(val) from values ('val Map(String, String)',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val Map(FixedString(1), FixedString(1))',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val Map(UInt64, UInt64)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(Date, Int16)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(DateTime(\'Asia/Istanbul\'), Int32)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(Enum16(\'a\'=1), Int16)',  (map('a', 1)), (map('a', 2)));
select maxMap(val) from values ('val Map(String, String)',  (map('1', '1')), (map('1', '2')));
select minMap(val) from values ('val Map(Int128, Int128)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(Int256, Int256)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(UInt128, UInt128)',  (map(1, 1)), (map(1, 2)));
select minMap(val) from values ('val Map(UInt256, UInt256)',  (map(1, 1)), (map(1, 2)));

select sumMap(map(1,2), 1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
select sumMap(map(1,2), map(1,3)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- array and tuple arguments
select avgMap([1,1,1], [2,2,2]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select minMap((1,1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select minMap(([1,1,1],1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select minMap([1,1,1],1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select minMap([1,1,1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select minMap(([1,1,1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS sum_map_decimal;

CREATE TABLE sum_map_decimal(statusMap Map(UInt16,Decimal32(5))) ENGINE = Log;

INSERT INTO sum_map_decimal VALUES (map(1,'1.0',2,'2.0',3,'3.0')), (map(3,'3.0',4,'4.0',5,'5.0')), (map(4,'4.0',5,'5.0',6,'6.0')), (map(6,'6.0',7,'7.0',8,'8.0'));

SELECT sumMap(statusMap) FROM sum_map_decimal;
SELECT sumWithOverflowMap(statusMap) FROM sum_map_decimal;

DROP TABLE sum_map_decimal;
