-- A parametrised alias on a whole ON condition or INTERPOLATE expression gets the same protective
-- parentheses as a plain alias. The SQL parser cannot produce this shape, so it is built through JSON.
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT * FROM a JOIN b ON (a.x = b.y AS x)'),
    '"alias":"x"', '"parametrised_alias":{"type":"QueryParameter","name":"x","param_type":"Identifier"}'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT n FROM t ORDER BY n WITH FILL INTERPOLATE (n AS (1 AS x))'),
    '"alias":"x"', '"parametrised_alias":{"type":"QueryParameter","name":"x","param_type":"Identifier"}'));
