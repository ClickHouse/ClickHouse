SET joined_subquery_requires_alias = 1;

SELECT * FROM (SELECT 1 as A, 2 as B) X
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B);

SELECT * FROM (SELECT 1 as A, 2 as B) X
ALL LEFT JOIN (SELECT 3 as A, 2 as B)
USING (B); -- { serverError 206 }

SELECT * FROM (SELECT 1 as A, 2 as B)
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B); -- { serverError 206 }

set joined_subquery_requires_alias = 0;

SELECT * FROM (SELECT 1 as A, 2 as B)
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B);
