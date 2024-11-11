-- https://github.com/ClickHouse/ClickHouse/issues/59154
SET allow_experimental_analyzer=1;
SELECT *
FROM
(
    WITH
        assumeNotNull((
            SELECT 0.9
        )) AS TUNING,
        ELEMENT_QUERY AS
        (
            SELECT quantiles(TUNING)(1)
        )
    SELECT *
    FROM ELEMENT_QUERY
);
