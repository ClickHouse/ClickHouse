WITH 'select count() from numbers((not 0 LIKE 1), 0)' AS q, formatQuery(q) AS q1, formatQuery(q1) AS q2 SELECT q1 == q2;
WITH 'SELECT * FROM not(1)' AS q, formatQuery(q) AS q1, formatQuery(q1) AS q2 SELECT q1 == q2;
