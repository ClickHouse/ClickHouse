-- { echo }

SELECT
    CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)')),
    CAST('33.3', 'Decimal64(1)') IN (CAST('33.30', 'Decimal64(2)'));

SELECT
    CAST('33.3','Decimal32(1)') IN (33.33),
    CAST('33.3','Decimal32(1)') IN (33.3);

SELECT
    CAST('33.3','Decimal64(1)') IN (33.33),
    CAST('33.3','Decimal64(1)') IN (33.3);

SELECT
    CAST('33.3','Decimal128(1)') IN (33.33),
    CAST('33.3','Decimal128(1)') IN (33.3);

SELECT
    CAST('33.3','Decimal256(1)') IN (33.33),
    CAST('33.3','Decimal256(1)') IN (33.3);


SELECT NULL IN 1;

SELECT CAST(NULL, 'Nullable(Int64)') IN (1.1);

SELECT CAST(NULL, 'Nullable(Int64)') IN (NULL, 1.1);

SELECT NULL IN (NULL);

SELECT NULL IN (NULL, 1);

SELECT (1, NULL) IN (1, 1);

SELECT (1, NULL) IN (1, NULL);

SELECT (1, NULL) IN ((1, 1), (NULL, 1));

SELECT NULL IN (1, 2, 3);

SELECT (1, NULL) IN [(1, 1)];


SET transform_null_in = 1;

SELECT NULL IN 1;

SELECT CAST(NULL, 'Nullable(Int64)') IN (1.1);

SELECT CAST(NULL, 'Nullable(Int64)') IN (NULL, 1.1);

SELECT NULL IN (NULL);

SELECT NULL IN (NULL, 1);

SELECT (1, NULL) IN (1, 1);

SELECT (1, NULL) IN (1, NULL);

SELECT (1, NULL) IN ((1, 1), (NULL, 1));

SELECT NULL IN (1, 2, 3);

SELECT (1, NULL) IN [(1, 1)];
