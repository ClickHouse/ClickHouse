SELECT product_id
FROM
(
    SELECT DISTINCT
        product_id,
        section_id
    FROM
    (
        SELECT
            concat('product_', number % 2) AS product_id,
            concat('section_', number % 3) AS section_id
        FROM numbers(10)
    )
)
SETTINGS enable_analyzer = 1;
