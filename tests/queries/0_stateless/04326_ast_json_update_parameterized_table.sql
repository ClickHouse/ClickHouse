-- The UPDATE target can be parameterized, and it must survive formatting,
-- including the JSON AST round-trip.
SELECT formatQuery('UPDATE {tbl:Identifier} SET x = 1 WHERE 1');
SELECT formatQuery('UPDATE {db:Identifier}.{tbl:Identifier} SET x = 1 WHERE 1');
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE {tbl:Identifier} SET x = 1 WHERE 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE {db:Identifier}.{tbl:Identifier} SET x = 1 WHERE 1'));
SELECT formatQueryFromJSON(parseQueryToJSON('UPDATE db.`tbl 1` SET x = 1 WHERE 1'));
