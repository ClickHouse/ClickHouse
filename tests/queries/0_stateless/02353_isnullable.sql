SELECT isNullable(3);
SELECT isNullable(toNullable(3));

SELECT isNullable(NULL);
SELECT isNullable(materialize(NULL));

SELECT isNullable(toLowCardinality(1));
SELECT isNullable(toNullable(toLowCardinality(1)));

SELECT isNullable(toLowCardinality(materialize(1)));
SELECT isNullable(toNullable(toLowCardinality(materialize(1))));
