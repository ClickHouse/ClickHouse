WITH map(1, 2, 3, NULL) AS m SELECT m[toNullable(1)], m[toNullable(2)], m[toNullable(3)];
WITH map(1, 2, 3, NULL) AS m SELECT m[materialize(toNullable(1))], m[materialize(toNullable(2))], m[materialize(toNullable(3))];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[toNullable(1)], m[toNullable(2)], m[toNullable(3)];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[materialize(toNullable(1))], m[materialize(toNullable(2))], m[materialize(toNullable(3))];

WITH map('a', 2, 'b', NULL) AS m SELECT m[toNullable('a')], m[toNullable('b')], m[toNullable('c')];
WITH map('a', 2, 'b', NULL) AS m SELECT m[materialize(toNullable('a'))], m[materialize(toNullable('b'))], m[materialize(toNullable('c'))];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[toNullable('a')], m[toNullable('b')], m[toNullable('c')];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[materialize(toNullable('a'))], m[materialize(toNullable('b'))], m[materialize(toNullable('c'))];

WITH map(1, 2, 3, NULL) AS m SELECT m[1], m[2], m[3];
WITH map(1, 2, 3, NULL) AS m SELECT m[materialize(1)], m[materialize(2)], m[materialize(3)];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[1], m[2], m[3];
WITH materialize(map(1, 2, 3, NULL)) AS m SELECT m[materialize(1)], m[materialize(2)], m[materialize(3)];

WITH map('a', 2, 'b', NULL) AS m SELECT m['a'], m['b'], m['c'];
WITH map('a', 2, 'b', NULL) AS m SELECT m[materialize('a')], m[materialize('b')], m[materialize('c')];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m['a'], m['b'], m['c'];
WITH materialize(map('a', 2, 'b', NULL)) AS m SELECT m[materialize('a')], m[materialize('b')], m[materialize('c')];
