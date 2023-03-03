SELECT * FROM
(
    SELECT reinterpretAsString(number + reinterpretAsUInt8('A')) AS k FROM system.numbers LIMIT 10
) js1
ALL LEFT JOIN
(
    SELECT reinterpretAsString(intDiv(number, 2) + reinterpretAsUInt8('A')) AS k, number AS joined FROM system.numbers LIMIT 10
) js2
USING k;
