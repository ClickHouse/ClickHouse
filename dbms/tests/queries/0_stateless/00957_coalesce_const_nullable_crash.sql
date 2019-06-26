SELECT coalesce(toNullable(1), toNullable(2)) as x, toTypeName(x);
SELECT coalesce(NULL, toNullable(2)) as x, toTypeName(x);
SELECT coalesce(toNullable(1), NULL) as x, toTypeName(x);
SELECT coalesce(NULL, NULL) as x, toTypeName(x);
