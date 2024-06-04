SELECT sum(number number number) FROM numbers(10); -- { clientError SYNTAX_ERROR }
SELECT sum(number number) FROM numbers(10); -- { clientError SYNTAX_ERROR }
SELECT sum(number AS number) FROM numbers(10);

SELECT [number number number] FROM numbers(1); -- { clientError SYNTAX_ERROR }
SELECT [number number] FROM numbers(1); -- { clientError SYNTAX_ERROR }
SELECT [number AS number] FROM numbers(1);

SELECT cast('1234' lhs lhs, 'UInt32'), lhs; -- { clientError SYNTAX_ERROR }