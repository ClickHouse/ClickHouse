SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (arrayJoin([1]) = B.b); -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON (A.a = arrayJoin([1])); -- { serverError 403 }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON equals(a); -- { serverError 62 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON less(a); -- { serverError 62 }

SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a > b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a < b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a >= b; -- { serverError 403 }
SELECT 1 FROM (select 1 a) A JOIN (select 1 b) B ON a = b AND a <= b; -- { serverError 403 }

SET join_algorithm = 'partial_merge';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError 48 }
-- works for a = b OR a = b because of equivalent disjunct optimization

SET join_algorithm = 'auto';
SELECT 1 FROM (select 1 a) A JOIN (select 1 b, 1 c) B ON a = b OR a = c; -- { serverError 48 }
-- works for a = b OR a = b because of equivalent disjunct optimization

SELECT 1 FROM (select 1 a, 1 aa, 1 aaa, 1 aaaa) A JOIN (select 1 b, 1 bb, 1 bbb, 1 bbbb, 1 bbbbb) B ON a = b OR a = bb OR a = bbb OR a = bbbb OR aa = b OR aa = bb OR aa = bbb OR aa = bbbb OR aaa = b OR aaa = bb OR aaa = bbb OR aaa = bbbb OR aaaa = b OR aaaa = bb OR aaaa = bbb OR aaaa = bbbb; -- { serverError 48 }


SET join_algorithm = 'hash';

SELECT 1 FROM (select 1 a, 1 aa, 1 aaa, 1 aaaa) A JOIN (select 1 b, 1 bb, 1 bbb, 1 bbbb, 1 bbbbb) B ON a = b OR a = bb OR a = bbb OR a = bbbb OR aa = b OR aa = bb OR aa = bbb OR aa = bbbb OR aaa = b OR aaa = bb OR aaa = bbb OR aaa = bbbb OR aaaa = b OR aaaa = bb OR aaaa = bbb OR aaaa = bbbb OR a = bbbbb OR aa = bbbbb; -- { serverError 403 }
