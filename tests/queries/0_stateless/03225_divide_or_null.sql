SELECT divideOrNull(1 , CAST(NULL, 'Nullable(Float32)'));
SELECT divideOrNull(materialize(1), CAST(NULL, 'Nullable(Float32)'));
SELECT divideOrNull(1, CAST(materialize(NULL), 'Nullable(Float32)'));
SELECT divideOrNull(materialize(1), CAST(materialize(NULL), 'Nullable(Float32)'));

SELECT divideOrNull(1, CAST(1, 'Nullable(Float32)')); -- NULL
SELECT divideOrNull(1, CAST(0, 'Nullable(Float32)'));  -- NULL
SELECT divideOrNull(materialize(1), CAST(1, 'Nullable(Float32)'));
SELECT divideOrNull(1, CAST(materialize(1), 'Nullable(Float32)'));
SELECT divideOrNull(materialize(1), CAST(materialize(1), 'Nullable(Float32)'));

SELECT divideOrNull(materialize(1), CAST(materialize(1), 'Float32'));
