SELECT toDecimal32OrZero('1.1', 1), toDecimal32OrZero('1.1', 2), toDecimal32OrZero('1.1', 8);
SELECT toDecimal32OrZero('1.1', 0);
SELECT toDecimal32OrZero(1.1, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT toDecimal128OrZero('', 0) AS x, toDecimal128OrZero('0.42', 2) AS y;
SELECT toDecimal64OrZero('', 0) AS x, toDecimal64OrZero('0.42', 3) AS y;
SELECT toDecimal32OrZero('', 0) AS x, toDecimal32OrZero('0.42', 4) AS y;

SELECT toDecimal32OrZero('999999999', 0), toDecimal32OrZero('1000000000', 0);
SELECT toDecimal32OrZero('-999999999', 0), toDecimal32OrZero('-1000000000', 0);
SELECT toDecimal64OrZero('999999999999999999', 0), toDecimal64OrZero('1000000000000000000', 0);
SELECT toDecimal64OrZero('-999999999999999999', 0), toDecimal64OrZero('-1000000000000000000', 0);
SELECT toDecimal128OrZero('99999999999999999999999999999999999999', 0);
SELECT toDecimal64OrZero('100000000000000000000000000000000000000', 0);
SELECT toDecimal128OrZero('-99999999999999999999999999999999999999', 0);
SELECT toDecimal64OrZero('-100000000000000000000000000000000000000', 0);

SELECT toDecimal32OrZero('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal64OrZero('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal128OrZero('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }

SELECT '----';

SELECT toDecimal32OrNull('1.1', 1), toDecimal32OrNull('1.1', 2), toDecimal32OrNull('1.1', 8);
SELECT toDecimal32OrNull('1.1', 0);
SELECT toDecimal32OrNull(1.1, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT toDecimal128OrNull('', 0) AS x, toDecimal128OrNull('-0.42', 2) AS y;
SELECT toDecimal64OrNull('', 0) AS x, toDecimal64OrNull('-0.42', 3) AS y;
SELECT toDecimal32OrNull('', 0) AS x, toDecimal32OrNull('-0.42', 4) AS y;

SELECT toDecimal32OrNull('999999999', 0), toDecimal32OrNull('1000000000', 0);
SELECT toDecimal32OrNull('-999999999', 0), toDecimal32OrNull('-1000000000', 0);
SELECT toDecimal64OrNull('999999999999999999', 0), toDecimal64OrNull('1000000000000000000', 0);
SELECT toDecimal64OrNull('-999999999999999999', 0), toDecimal64OrNull('-1000000000000000000', 0);
SELECT toDecimal128OrNull('99999999999999999999999999999999999999', 0);
SELECT toDecimal64OrNull('100000000000000000000000000000000000000', 0);
SELECT toDecimal128OrNull('-99999999999999999999999999999999999999', 0);
SELECT toDecimal64OrNull('-100000000000000000000000000000000000000', 0);

SELECT toDecimal32OrNull('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal64OrNull('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
SELECT toDecimal128OrNull('1', rowNumberInBlock()); -- { serverError ILLEGAL_COLUMN }
