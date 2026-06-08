-- { echoOn }

SELECT 1.1::Decimal(154, 76);
SELECT round(1.1::Decimal(154, 76));
SELECT round(1.1::Decimal(154, 76), 1);
SELECT round(1.234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901::Decimal(154, 76), 1);
SELECT round(1.234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901::Decimal(154, 76), 76);
SELECT round(1.234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901::Decimal(154, 76), 77);
SELECT round(1.234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901::Decimal(154, 76), 100);

SELECT hex(1.2345678901234567890123456789012345678901234567890123456789012345678901234567::Decimal(154, 76));
SELECT bin(1.2345678901234567890123456789012345678901234567890123456789012345678901234567::Decimal(154, 76));
SELECT reinterpret(unhex(hex(1.2345678901234567890123456789012345678901234567890123456789012345678901234567::Decimal(154, 76))), 'Decimal(154, 76)');

SELECT arraySum([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arraySum([1.2::Decimal(154, 76), 3.45::Decimal(3, 2)]);

SELECT arrayMin([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arrayMax([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arrayAvg([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);

SELECT 1.4731307714040098e76;
SELECT toTypeName(arrayProduct([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]));

-- SELECT arrayCumSum([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arrayCumSum([1.2::Decimal(60, 30), 3.45::Decimal(61, 29)]);
-- SELECT arrayCumSumNonNegative([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arrayCumSumNonNegative([1.2::Decimal(60, 30), 3.45::Decimal(61, 29)]);
-- SELECT arrayDifference([1.2::Decimal(154, 76), 3.45::Decimal(154, 76)]);
SELECT arrayDifference([1.2::Decimal(60, 30), 3.45::Decimal(61, 29)]);

SELECT arrayCompact([1.2::Decimal(154, 76) AS x, x, x, x, 3.45::Decimal(3, 2) AS y, y, x, x]);

SELECT 1.2::Decimal(2, 1) IN (1.2::Decimal(154, 76), 3.4::Decimal(154, 76));
SELECT 1.23::Decimal(3, 2) IN (1.2::Decimal(154, 76), 3.4::Decimal(154, 76));
SELECT 1.2::Decimal(154, 76) IN (1.2::Decimal(2, 1));

SELECT toTypeName([1.2::Decimal(154, 76), 3.45::Decimal(3, 2)]);
SELECT toTypeName(arraySum([1.2::Decimal(154, 76), 3.45::Decimal(3, 2)]));

SELECT arrayJoin(sumMap(x)) FROM (SELECT [('Hello', 1.2::Decimal512(76)), ('World', 3.4::Decimal512(76))]::Map(String, Decimal512(76)) AS x UNION ALL SELECT [('World', 5.6::Decimal512(76)), ('GoodBye', -111.222::Decimal512(76))]::Map(String, Decimal512(76))) ORDER BY 1;

-- SELECT mapAdd(map('Hello', 1.2::Decimal512(76), 'World', 3.4::Decimal512(76)), map('World', 5.6::Decimal512(76), 'GoodBye', -111.222::Decimal512(76)));
SELECT mapAdd(map('Hello', 1.2::Decimal128(30), 'World', 3.4::Decimal128(30)), map('World', 5.6::Decimal128(30), 'GoodBye', -111.222::Decimal128(30)));
-- SELECT mapSubtract(map('Hello', 1.2::Decimal512(76), 'World', 3.4::Decimal512(76)), map('World', 5.6::Decimal512(76), 'GoodBye', -111.222::Decimal512(76)));
SELECT mapSubtract(map('Hello', 1.2::Decimal128(30), 'World', 3.4::Decimal128(30)), map('World', 5.6::Decimal128(30), 'GoodBye', -111.222::Decimal128(30)));

SELECT mapAdd(map('Hello', 1.2::Decimal256(30), 'World', 3.4::Decimal256(30)), map('World', 5.6::Decimal256(30), 'GoodBye', -111.222::Decimal256(30)));
SELECT mapSubtract(map('Hello', 1.2::Decimal256(30), 'World', 3.4::Decimal256(30)), map('World', 5.6::Decimal256(30), 'GoodBye', -111.222::Decimal256(30)));

-- SELECT arraySort(arrayIntersect([1.1::Decimal512(150), 2.34::Decimal512(140), 3.456::Decimal512(130)], [2.34::Decimal512(145), 3.456::Decimal512(135), 4.5678::Decimal512(125)]));
SELECT arraySort(arrayIntersect([1.1::Decimal512(1)], [1.12::Decimal512(2)]));
SELECT arraySort(arrayIntersect([1.1::Decimal512(2)], [1.12::Decimal512(2)]));
SELECT arraySort(arrayIntersect([1.1::Decimal512(1)], [1.12::Decimal512(2)]));
SELECT arraySort(arrayIntersect([1.1::Decimal512(2)], [1.12::Decimal512(2)]));
-- SELECT arraySort(arrayIntersect([1.1::Decimal256(70), 2.34::Decimal256(60), 3.456::Decimal256(50)], [2.34::Decimal256(65), 3.456::Decimal256(55), 4.5678::Decimal256(45)]));
-- SELECT arraySort(arrayIntersect([1.1::Decimal256(1)], [1.12::Decimal256(2)])); -- Note: this is correct but the semantics has to be clarified in the docs.
-- SELECT arraySort(arrayIntersect([1.1::Decimal256(2)], [1.12::Decimal256(2)]));
-- SELECT arraySort(arrayIntersect([1.1::Decimal128(1)], [1.12::Decimal128(2)])); -- Note: this is correct but the semantics has to be clarified in the docs.
-- SELECT arraySort(arrayIntersect([1.1::Decimal128(2)], [1.12::Decimal128(2)]));



select coalesce(cast('123', 'Nullable(Decimal(154, 76))'), 0);
select coalesce(cast('123', 'Nullable(Decimal(154, 76))'), 0);
select coalesce(cast('123', 'Decimal(154, 76)'), 0);

DROP TABLE IF EXISTS decimal_insert_cast_issue;
create table decimal_insert_cast_issue (a Decimal(154, 0)) engine = TinyLog;
SET param_param = 1;
INSERT INTO decimal_insert_cast_issue VALUES ({param:Nullable(Decimal(154, 0))});
SELECT * FROM decimal_insert_cast_issue;
DROP TABLE decimal_insert_cast_issue;