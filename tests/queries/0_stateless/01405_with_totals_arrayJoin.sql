SELECT 'ARRAY JOIN';
SELECT * FROM numbers(1) ARRAY JOIN [[], []] AS a_ GROUP BY number WITH TOTALS;
-- before there was:
--     Code: 10. DB::Exception: Received from localhost:9000. DB::Exception: Not found column a_ in block. There are only columns: number.
SELECT *, a_ FROM numbers(1) ARRAY JOIN [[], []] AS a_ GROUP BY number WITH TOTALS; -- { serverError 215; }
SELECT *, any(a_) FROM numbers(1) ARRAY JOIN [[], []] AS a_ GROUP BY number WITH TOTALS;
SELECT 'arrayJoin';
SELECT arrayJoin([[], []]), * FROM numbers(1) GROUP BY number WITH TOTALS; -- { serverError 215; }
-- after https://github.com/ClickHouse/ClickHouse/pull/13681
-- (do not forget to update .reference)
-- SELECT any(arrayJoin([[], []])), * FROM numbers(1) GROUP BY number WITH TOTALS;
