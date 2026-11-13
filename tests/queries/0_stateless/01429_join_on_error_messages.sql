SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON equals(a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, 62 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON less(a); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH, 62 }

SET join_algorithm = 'partial_merge';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'grace_hash';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError NOT_IMPLEMENTED }
-- works for a = b OR a = b because of equivalent disjunct optimization
