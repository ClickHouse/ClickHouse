SELECT isNullable(3);
SELECT isNullable(toNullable(3));

SELECT isNullable(NULL);
SELECT isNullable(materialize(NULL));
