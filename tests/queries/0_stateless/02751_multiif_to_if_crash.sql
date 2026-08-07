SELECT sum(A) FROM (SELECT multiIf(1, 1, NULL) as A);
SELECT sum(multiIf(number = NULL, 65536, 3))  FROM numbers(3);
SELECT multiIf(NULL, 65536 :: UInt32, 3 :: Int32);
