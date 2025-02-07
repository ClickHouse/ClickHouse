SELECT formatQuery('ALTER TABLE t (UPDATE c = (1 AS a) WHERE true)')
SELECT formatQuery('ALTER TABLE t (UPDATE c = a > 1 WHERE true)')
