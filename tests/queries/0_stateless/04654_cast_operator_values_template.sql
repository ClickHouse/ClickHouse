-- The `::` cast operator keeps its type as a string, so it builds new literals right after throwing
-- away the AST of the type it parsed. The literals inside that AST - the scale of a `Decimal32(3)`,
-- for instance - are recorded in the parser's literal token map, and the allocator readily hands
-- their addresses to the next literal. A literal inheriting the token range of a type argument
-- makes `ValuesBlockInputFormat` deduce an expression template that replaces the type argument,
-- which then fails to parse and drops the row onto a slower path with different conversion rules.

SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

CREATE TEMPORARY TABLE t (d Decimal64(3), tm Time64(3), dt DateTime64(3));

INSERT INTO t VALUES (1.5::Decimal32(3), 36610.111::Decimal32(3), 33010.111::Decimal32(3));
INSERT INTO t VALUES (2.5::Decimal32(3), 36620.222::Decimal32(3), 33020.222::Decimal32(3));

SELECT * FROM t ORDER BY d;

SELECT 1.5::Decimal(9, 3), '2.5'::Decimal64(4), [1, 2]::Array(UInt8);
