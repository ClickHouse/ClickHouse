-- { echoOn }

SET enable_analyzer = 1;

SELECT (y -> 1) IN (1, 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT (y -> 1) IN (materialize(1), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT (y -> 1) IN [materialize(1), 2]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 1 WHERE (y -> 1) IN (materialize(1), 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
