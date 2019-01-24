SELECT a.*, b.* FROM
(
    SELECT reinterpretAsString(number + reinterpretAsUInt8('A')) AS k FROM system.numbers LIMIT 10
) AS a
ALL LEFT JOIN
(
    SELECT reinterpretAsString(intDiv(number, 2) + reinterpretAsUInt8('A')) AS k, number AS joined FROM system.numbers LIMIT 10
) AS b
USING k;
