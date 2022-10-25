SET use_analyzer = 1;

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

SELECT (1, 2) IN 1; -- { serverError 43 }
SELECT (1, 2) IN [1]; -- { serverError 124 }
SELECT (1, 2) IN (((1, 2), (1, 2)), ((1, 2), (1, 2))); -- { serverError 43 }
SELECT (1, 2) IN [((1, 2), (1, 2)), ((1, 2), (1, 2))]; -- { serverError 43 }
