SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (arrayJoin([1]) = B.b); -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (A.a = arrayJoin([1])); -- { serverError 403 }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON equals(a); -- { serverError 62 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON less(a); -- { serverError 62 }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a > b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a < b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a >= b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a <= b; -- { serverError 403 }

SET join_algorithm = 'partial_merge';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b OR a = b; -- { serverError 48 }

SET join_algorithm = 'auto';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b OR a = b; -- { serverError 48 }

SET join_algorithm = 'hash';
