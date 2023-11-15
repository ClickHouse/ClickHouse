SELECT * FROM ( SELECT 1 as a ) as t1 JOIN ( SELECT 1 as a) as t2 ON arrayJoin([t1.a]) = t2.a; -- { serverError INVALID_JOIN_ON_EXPRESSION }
