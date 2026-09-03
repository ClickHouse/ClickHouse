SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_hash_in;
CREATE TABLE row_hash_in (a UInt64, r Row(x UInt64, y String), ar Array(Row(x UInt64, y String))) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_hash_in VALUES (1, (1, 'a'), [(1, 'a'), (2, 'b')]), (2, (1, 'b'), [(1, 'b')]), (3, (0, 'z'), []);

-- Hash functions flatten a Row like the equivalent Tuple, so both hash the same.
SELECT a, sipHash64(r) = sipHash64(r::Tuple(x UInt64, y String)), cityHash64(ar) = cityHash64(ar::Array(Tuple(x UInt64, y String))) FROM row_hash_in ORDER BY a;
SELECT sipHash64((1::UInt64, 'a')::Row(x UInt64, y String)) = sipHash64((1::UInt64, 'a')), sipHash64Keyed((1::UInt64, 2::UInt64), (1::UInt64, 'a')::Row(x UInt64, y String)) = sipHash64Keyed((1::UInt64, 2::UInt64), (1::UInt64, 'a'));

-- A Row nested inside the left-hand side of IN is lowered like a bare Row.
SELECT a, ar IN ([(1, 'a'), (2, 'b')]), ar IN ([(1, 'a'), (2, 'b')], [(1, 'b')]) FROM row_hash_in ORDER BY a;
SELECT a, (r, a) IN (((1, 'a'), 1), ((0, 'z'), 3)) FROM row_hash_in ORDER BY a;
SELECT a, [ar] IN ([[(1, 'b')]]), map(r, a) IN (map((1, 'b'), 2)) FROM row_hash_in ORDER BY a;
SELECT a FROM row_hash_in WHERE ar IN ([(0, 'q')], [(1, 'b')]) ORDER BY a;

DROP TABLE row_hash_in;
