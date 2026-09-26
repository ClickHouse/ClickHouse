DROP TABLE IF EXISTS t_vrow;

CREATE TABLE t_vrow (CounterID UInt32, CounterID2 UInt32)
ENGINE = MergeTree ORDER BY CounterID;

SYSTEM STOP MERGES t_vrow;

-- Many small parts with overlapping ranges
INSERT INTO t_vrow SELECT sipHash64(number, 0) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 1) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 2) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 3) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 4) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 5) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 6) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 7) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 8) % 100000, rand32() % 100000 FROM numbers(100000);
INSERT INTO t_vrow SELECT sipHash64(number, 9) % 100000, rand32() % 100000 FROM numbers(100000);

SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1;

SELECT CounterID FROM t_vrow ORDER BY CounterID DESC LIMIT 100;
SELECT CounterID FROM t_vrow ORDER BY CounterID ASC LIMIT 100;

SELECT * FROM t_vrow ORDER BY CounterID2 DESC FORMAT Null;
SELECT * FROM t_vrow ORDER BY CounterID2 ASC FORMAT Null;

DROP TABLE t_vrow;
