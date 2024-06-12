SELECT sum(number number number) FROM numbers(10); -- { clientError 62 }
SELECT sum(number number) FROM numbers(10); -- { clientError 62 }
SELECT sum(number AS number) FROM numbers(10);

SELECT [number number number] FROM numbers(1); -- { clientError 62 }
SELECT [number number] FROM numbers(1); -- { clientError 62 }
SELECT [number AS number] FROM numbers(1);

SELECT cast('1234' lhs lhs, 'UInt32'), lhs; -- { clientError 62 }