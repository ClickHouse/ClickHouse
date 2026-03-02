-- With the NOT operator precedence fix, (SELECT ...) after NOT is correctly parsed as a subquery.
-- Previously NOT (SELECT ...) was parsed as not(SELECT ...) where SELECT was an identifier.

SELECT 1 where (NOT (SELECT [69::UInt64][1])); -- now this correctly evaluates the subquery

SELECT 1 where (NOT (SELECT [[69::UInt64]][1])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
