SET enable_analyzer = 1;

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

select (select 1) in (1);
select in(untuple(((1), (1))));
select in(untuple(((select 1), (1))));
