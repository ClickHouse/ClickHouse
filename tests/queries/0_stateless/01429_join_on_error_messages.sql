SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (arrayJoin([1]) = B.b); -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (A.a = arrayJoin([1])); -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON equals(a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, 62 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON less(a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, 62 }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a > b; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a < b; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a >= b; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a <= b; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SET join_algorithm = 'partial_merge';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'grace_hash';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'hash';

-- conditions for different table joined via OR
SELECT * FROM (SELECT 1 AS a, 1 AS b, 1 AS c) AS t1 INNER JOIN (SELECT 1 AS a, 1 AS b, 1 AS c) AS t2 ON t1.a = t2.a AND (t1.b > 0 OR t2.b > 0); -- { serverError INVALID_JOIN_ON_EXPRESSION }
