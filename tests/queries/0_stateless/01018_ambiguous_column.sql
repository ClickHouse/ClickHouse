SET output_format_pretty_color=1;
SET enable_analyzer = 1;

select * from system.one cross join system.one;
select * from system.one cross join system.one r;
select * from system.one l cross join system.one;
select * from system.one left join system.one using dummy;
select dummy from system.one left join system.one using dummy;

USE system;

SELECT dummy FROM one AS A JOIN one ON A.dummy = one.dummy;
SELECT dummy FROM one JOIN one AS A ON A.dummy = one.dummy;
SELECT dummy FROM one l JOIN one r ON dummy = r.dummy;
SELECT dummy FROM one l JOIN one r ON l.dummy = dummy; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT dummy FROM one l JOIN one r ON one.dummy = r.dummy;
SELECT dummy FROM one l JOIN one r ON l.dummy = one.dummy; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT * from one
JOIN one A ON one.dummy = A.dummy
JOIN one B ON one.dummy = B.dummy
FORMAT PrettyCompact;

SELECT * from one A
JOIN system.one one ON A.dummy = one.dummy
JOIN system.one two ON A.dummy = two.dummy
FORMAT PrettyCompact;

-- SELECT one.dummy FROM one AS A FULL JOIN (SELECT 0 AS dymmy) AS one USING dummy;
SELECT one.dummy FROM one AS A JOIN (SELECT 0 AS dummy) B USING dummy;
