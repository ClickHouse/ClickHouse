DROP FUNCTION IF EXISTS 03215_udf_with_union;
CREATE FUNCTION 03215_udf_with_union AS () -> (
    SELECT sum(s)
    FROM
    (
        SELECT 1 AS s
        UNION ALL
        SELECT 1 AS s
    )
);

SELECT 03215_udf_with_union();

DROP FUNCTION 03215_udf_with_union;
