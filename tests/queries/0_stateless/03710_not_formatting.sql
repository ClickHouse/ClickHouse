with 'select count() from numbers((not 0 LIKE 1), 0)' as q, formatQuery(q) as q1, formatQuery(q1) as q2 SELECT q1 == q2;
