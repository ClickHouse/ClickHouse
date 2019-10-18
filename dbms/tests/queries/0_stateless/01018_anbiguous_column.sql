select * from system.one cross join system.one; -- { serverError 352 }
select * from system.one cross join system.one r;
select * from system.one l cross join system.one;
select * from system.one left join system.one using dummy;
select dummy from system.one left join system.one using dummy;

USE system;

SELECT dummy FROM one AS A JOIN one ON A.dummy = one.dummy;
SELECT dummy FROM one JOIN one AS A ON A.dummy = one.dummy;
-- SELECT dummy FROM one l JOIN one r ON l.dummy = r.dummy; -- should be an error

-- SELECT * from system.one
-- JOIN system.one one ON one.dummy = system.one.dummy
-- JOIN system.one two ON one.dummy = two.dummy
-- FORMAT PrettyCompact;

-- SELECT * from system.one one
-- JOIN system.one ON one.dummy = system.one.dummy
-- JOIN system.one two ON one.dummy = two.dummy
-- FORMAT PrettyCompact;
