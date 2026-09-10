SELECT DISTINCT(i_product_name)
FROM item AS i1
WHERE (i_manufact_id BETWEEN 738 AND 738 + 40)
    AND (
        SELECT count(*) AS item_cnt
        FROM item
        WHERE (
            (i_manufact = i1.i_manufact)
            AND (
                (
                    (i_category = 'Women')
                    AND (i_color = 'powder' OR i_color = 'khaki')
                    AND (i_units = 'Ounce' OR i_units = 'Oz')
                    AND (i_size = 'medium' OR i_size = 'extra large')
                )
                OR (
                    (i_category = 'Women')
                    AND (i_color = 'brown' OR i_color = 'honeydew')
                    AND (i_units = 'Bunch' OR i_units = 'Ton')
                    AND (i_size = 'N/A' OR i_size = 'small')
                )
                OR (
                    (i_category = 'Men')
                    AND (i_color = 'floral' OR i_color = 'deep')
                    AND (i_units = 'N/A' OR i_units = 'Dozen')
                    AND (i_size = 'petite' OR i_size = 'large')
                )
                OR (
                    (i_category = 'Men')
                    AND (i_color = 'light' OR i_color = 'cornflower')
                    AND (i_units = 'Box' OR i_units = 'Pound')
                    AND (i_size = 'medium' OR i_size = 'extra large')
                )
            )
        )
        OR (
            (i_manufact = i1.i_manufact)
            AND (
                (
                    (i_category = 'Women')
                    AND (i_color = 'midnight' OR i_color = 'snow')
                    AND (i_units = 'Pallet' OR i_units = 'Gross')
                    AND (i_size = 'medium' OR i_size = 'extra large')
                )
                OR (
                    (i_category = 'Women')
                    AND (i_color = 'cyan' OR i_color = 'papaya')
                    AND (i_units = 'Cup' OR i_units = 'Dram')
                    AND (i_size = 'N/A' OR i_size = 'small')
                )
                OR (
                    (i_category = 'Men')
                    AND (i_color = 'orange' OR i_color = 'frosted')
                    AND (i_units = 'Each' OR i_units = 'Tbl')
                    AND (i_size = 'petite' OR i_size = 'large')
                )
                OR (
                    (i_category = 'Men')
                    AND (i_color = 'forest' OR i_color = 'ghost')
                    AND (i_units = 'Lb' OR i_units = 'Bundle')
                    AND (i_size = 'medium' OR i_size = 'extra large')
                )
            )
        )
    ) > 0
ORDER BY i_product_name
LIMIT 100;
