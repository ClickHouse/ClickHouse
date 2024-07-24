SELECT maxMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT maxMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT maxMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT '-';

SELECT maxMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT maxMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT maxMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number :: Float64, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT '-';

SELECT maxMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT maxMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT maxMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT minMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT minMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT minMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;

SELECT sumMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH TOTALS;
SELECT sumMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH ROLLUP;
SELECT sumMap([number % 3, number % 4 - 1], [number :: UInt256, NULL]) FROM numbers(3) GROUP BY number WITH CUBE;
