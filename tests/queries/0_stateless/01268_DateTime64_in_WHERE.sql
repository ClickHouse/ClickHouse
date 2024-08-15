-- Error cases:
-- non-const string column
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT DT64 = materialize(S); -- {serverError 43}
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT materialize(S) = toDateTime64(S, 3); -- {serverError 43}
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 = materialize(S); -- {serverError 43}
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE materialize(S) = DT64; -- {serverError 43}

SELECT * WHERE toDateTime64(123.345, 3) == 'ABCD'; -- {serverError 53} -- invalid DateTime64 string
SELECT * WHERE toDateTime64(123.345, 3) == '2020-02-05 14:34:12.33333333333333333333333333333333333333333333333333333333';

SELECT 'in SELECT';
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT DT64 = S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT S = DT64;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT materialize(DT64) = S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT S = materialize(DT64);

SELECT 'in WHERE';
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 = S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S = DT64;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE materialize(DT64) = S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S = materialize(DT64);

SELECT 'other operators';
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 <= S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 >= S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S <= DT64;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S >= DT64;

-- empty results
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 < S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 > S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE DT64 != S;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S < DT64;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S > DT64;
WITH '2020-02-05 14:34:12.333' as S, toDateTime64(S, 3) as DT64 SELECT * WHERE S != DT64;
