SELECT *
FROM merge('system', '^one$') AS one
WHERE (one.dummy = 0) OR (one.dummy = 1);
