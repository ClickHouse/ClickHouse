DROP TABLE IF EXISTS 03314_divide_decimal_short_circuit;
CREATE TABLE 03314_divide_decimal_short_circuit(
    `n1` Decimal(38,2),
    `n2` Decimal(38,2)
) ENGINE=Memory;

INSERT INTO 03314_divide_decimal_short_circuit VALUES
    (7, 0),
    (0.07, 0),
    (0.07, 0.01),
    (0.07, -70),
    (0.07, 1506)
;

SELECT n1, n2, multiIf(n2 != 0, n1 / n2, 0), multiIf(n2 != 0, divideDecimal(n1, n2, 6), 0)
FROM 03314_divide_decimal_short_circuit;
