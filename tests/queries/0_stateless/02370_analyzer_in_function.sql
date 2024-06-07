SET allow_experimental_analyzer = 1;

SELECT 1 IN 1;
SELECT 1 IN (1);
SELECT 1 IN 0;
SELECT 1 IN (0);
SELECT 1 IN (1, 2);
SELECT (1, 1) IN ((1, 1), (1, 2));
SELECT (1, 1) IN ((1, 2), (1, 2));
SELECT 1 IN (((1), (2)));

SELECT '--';

SELECT 1 IN [1];
SELECT 1 IN [0];
SELECT 1 IN [1, 2];
SELECT (1, 1) IN [(1, 1), (1, 2)];
SELECT (1, 1) IN [(1, 2), (1, 2)];

SELECT (1, 2) IN 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (1, 2) IN [1]; -- { serverError INCORRECT_ELEMENT_OF_SET }
SELECT (1, 2) IN (((1, 2), (1, 2)), ((1, 2), (1, 2))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (1, 2) IN [((1, 2), (1, 2)), ((1, 2), (1, 2))]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
