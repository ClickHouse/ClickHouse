# Test WITH FILL without INTERPOLATE
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5;

# Test INTERPOLATE with const
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS 42);

# Test INTERPOLATE with field value
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS inter);

# Test INTERPOLATE with expression
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS inter + 1);

# Test INTERPOLATE with incompatible const - should produce error
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS 'inter'); -- { serverError CANNOT_PARSE_TEXT }

# Test INTERPOLATE with incompatible expression - should produce error
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS reverse(inter)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

# Test INTERPOLATE with column from WITH FILL expression - should produce error
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (n AS n); -- { serverError INVALID_WITH_FILL_EXPRESSION }

# Test INTERPOLATE with inconsistent column - should produce error
SELECT n, source, inter FROM (
   SELECT toFloat32(number % 10) AS n, 'original' AS source, number as inter FROM numbers(10) WHERE number % 3 = 1
) ORDER BY n WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE (inter AS source); -- { serverError CANNOT_PARSE_TEXT, 32 }

# Test INTERPOLATE with aliased column
SELECT n, source, inter + 1 AS inter_p FROM (
    SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter FROM numbers(10) WHERE (number % 3) = 1
) ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter_p AS inter_p + 1 );

# Test INTERPOLATE with column not present in select
SELECT source, inter FROM (
    SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter, number + 1 AS inter2 FROM numbers(10) WHERE (number % 3) = 1
) ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter AS inter2 + inter );

# Test INTERPOLATE in sub-select
SELECT n, source, inter FROM (
    SELECT n, source, inter, inter2 FROM (
        SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter, number + 1 AS inter2 FROM numbers(10) WHERE (number % 3) = 1
    ) ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter AS inter + inter2 )
);

# Test INTERPOLATE with aggregates
SELECT n, any(source), sum(inter) AS inter_s FROM (
    SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter FROM numbers(10) WHERE (number % 3) = 1
) GROUP BY n
ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter_s AS inter_s + 1 );

# Test INTERPOLATE with Nullable in result
SELECT n, source, inter + NULL AS inter_p FROM (
    SELECT toFloat32(number % 10) AS n, 'original' AS source, number AS inter FROM numbers(10) WHERE (number % 3) = 1
) ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter_p AS inter_p + 1 );

# Test INTERPOLATE with Nullable in source
SELECT n, source, inter AS inter_p FROM (
    SELECT toFloat32(number % 10) AS n, 'original' AS source, number + NULL AS inter FROM numbers(10) WHERE (number % 3) = 1
) ORDER BY n ASC WITH FILL FROM 0 TO 11.51 STEP 0.5 INTERPOLATE ( inter_p AS inter_p + 1 );

# Test INTERPOLATE for MergeTree
DROP TABLE IF EXISTS t_inter_02233;
CREATE TABLE t_inter_02233 (n Int32) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_inter_02233 VALUES (1),(3),(3),(6),(6),(6);
SELECT n, count() AS m FROM t_inter_02233 GROUP BY n ORDER BY n WITH FILL INTERPOLATE ( m AS m + 1 );
DROP TABLE IF EXISTS t_inter_02233;
