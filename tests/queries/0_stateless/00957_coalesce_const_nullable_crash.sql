SELECT coalesce(toNullable(1), toNullable(2)) as x, toTypeName(x);
SELECT coalesce(NULL, toNullable(2)) as x, toTypeName(x);
SELECT coalesce(toNullable(1), NULL) as x, toTypeName(x);
SELECT coalesce(NULL, NULL) as x, toTypeName(x);

SELECT coalesce(toNullable(materialize(1)), toNullable(materialize(2))) as x, toTypeName(x);
SELECT coalesce(NULL, toNullable(materialize(2))) as x, toTypeName(x);
SELECT coalesce(toNullable(materialize(1)), NULL) as x, toTypeName(x);
SELECT coalesce(materialize(NULL), materialize(NULL)) as x, toTypeName(x);

SELECT coalesce(toLowCardinality(toNullable(1)), toLowCardinality(toNullable(2))) as x, toTypeName(x);
SELECT coalesce(NULL, toLowCardinality(toNullable(2))) as x, toTypeName(x);
SELECT coalesce(toLowCardinality(toNullable(1)), NULL) as x, toTypeName(x);
