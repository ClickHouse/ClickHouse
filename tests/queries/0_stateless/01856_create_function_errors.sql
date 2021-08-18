-- CREATE FUNCTION MyFunc2 AS (a, b) -> a || b || c; --{serverError 47}

-- CREATE FUNCTION MyFunc2 AS (a, b) -> MyFunc2(a, b) + MyFunc2(a, b); --{serverError 600}

-- CREATE FUNCTION cast AS a -> a + 1; --{serverError 598}

-- CREATE FUNCTION sum AS (a, b) -> a + b; --{serverError 598}

-- CREATE FUNCTION MyFunc3 AS (a, b) -> a + b;

-- CREATE FUNCTION MyFunc3 AS (a) -> a || '!!!'; --{serverError 598}

-- DROP FUNCTION MyFunc3;

-- DROP FUNCTION unknownFunc; -- {serverError 46}

DROP FUNCTION CAST; -- {serverError 599}
