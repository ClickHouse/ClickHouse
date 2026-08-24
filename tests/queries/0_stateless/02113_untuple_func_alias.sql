SELECT untuple((1, 2, 3, b)) AS `ut`, untuple((NULL, 3, 2, a)) AS `ut2`
FROM (SELECT 1 AS a, NULL AS b) FORMAT TSVWithNames;
