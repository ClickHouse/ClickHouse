SELECT toNullable(0) + 1 AS x, toTypeName(x), toColumnTypeName(x);
SELECT toNullable(materialize(0)) + 1 AS x, toTypeName(x), toColumnTypeName(x);
SELECT materialize(toNullable(0)) + 1 AS x, toTypeName(x), toColumnTypeName(x);
SELECT toNullable(0) + materialize(1) AS x, toTypeName(x), toColumnTypeName(x);
SELECT toNullable(materialize(0)) + materialize(1) AS x, toTypeName(x), toColumnTypeName(x);
SELECT materialize(toNullable(0)) + materialize(1) AS x, toTypeName(x), toColumnTypeName(x);
