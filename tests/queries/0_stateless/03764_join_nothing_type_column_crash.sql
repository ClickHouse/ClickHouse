-- Test for bug https://github.com/ClickHouse/ClickHouse/issues/92043
SELECT DISTINCT kafkaMurmurHash(*) FROM numbers(10) y ANY RIGHT JOIN (SELECT concat('str', number) str, arrayJoin(range(number)) i, number FROM numbers(10)) AS z ON y.number = z.number ORDER BY 1;
