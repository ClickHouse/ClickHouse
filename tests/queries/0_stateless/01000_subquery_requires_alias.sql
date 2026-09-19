SET enable_analyzer = 1;
SET joined_subquery_requires_alias = 1;

SELECT * FROM (SELECT 1 as A, 2 as B) X
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B);

SELECT * FROM (SELECT 1 as A, 2 as B) X
ALL LEFT JOIN (SELECT 3 as A, 2 as B)
USING (B); -- { serverError ALIAS_REQUIRED }

-- The unaliased left side is not ambiguous here: `A` binds to it and the right column is qualified as `Y.A`.
SELECT * FROM (SELECT 1 as A, 2 as B)
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B);

set joined_subquery_requires_alias = 0;

SELECT * FROM (SELECT 1 as A, 2 as B)
ALL LEFT JOIN (SELECT 3 as A, 2 as B) Y
USING (B);
