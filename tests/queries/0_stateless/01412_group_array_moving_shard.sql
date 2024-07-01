-- Tags: shard

SELECT groupArrayMovingSum(10)(1) FROM remote('127.0.0.{1,2}', numbers(100));
SELECT groupArrayMovingAvg(10)(1) FROM remote('127.0.0.{1,2}', numbers(100));

SELECT groupArrayMovingSum(256)(-1) FROM numbers(300);
SELECT groupArrayMovingSum(256)(-1) FROM remote('127.0.0.{1,2}', numbers(200));
SELECT groupArrayMovingAvg(256)(1) FROM numbers(300);

SELECT groupArrayMovingSum(256)(toDecimal32(100000000, 1)) FROM numbers(300);
SELECT groupArrayMovingSum(256)(toDecimal64(-1, 1)) FROM numbers(300);
SELECT groupArrayMovingAvg(256)(toDecimal128(-1, 1)) FROM numbers(300);


SELECT groupArrayMovingSum(10)(number) FROM numbers(100);
SELECT groupArrayMovingSum(10)(1) FROM numbers(100);
SELECT groupArrayMovingSum(0)(1) FROM numbers(100); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0.)(1) FROM numbers(100); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0.1)(1) FROM numbers(100); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(0.1)(1) FROM remote('127.0.0.{1,2}', numbers(100)); -- { serverError BAD_ARGUMENTS }
SELECT groupArrayMovingSum(256)(1) FROM remote('127.0.0.{1,2}', numbers(100));
SELECT groupArrayMovingSum(256)(1) FROM remote('127.0.0.{1,2}', numbers(1000));
SELECT toTypeName(groupArrayMovingSum(256)(-1)) FROM remote('127.0.0.{1,2}', numbers(1000));
SELECT groupArrayMovingSum(256)(toDecimal32(1, 9)) FROM numbers(300);
SELECT groupArrayMovingSum(256)(toDecimal32(1000000000, 1)) FROM numbers(300); -- { serverError DECIMAL_OVERFLOW }
SELECT groupArrayMovingSum(256)(toDecimal32(100000000, 1)) FROM numbers(300);
SELECT groupArrayMovingSum(256)(toDecimal32(1, 1)) FROM numbers(300);

SELECT groupArrayMovingAvg(256)(1) FROM remote('127.0.0.{1,2}', numbers(1000));
SELECT groupArrayMovingAvg(256)(-1) FROM numbers(300);
SELECT arrayMap(x -> round(x, 4), groupArrayMovingAvg(256)(1)) FROM numbers(300);
SELECT groupArrayMovingAvg(256)(toDecimal32(1, 9)) FROM numbers(300);
SELECT toTypeName(groupArrayMovingAvg(256)(toDecimal32(1, 9))) FROM numbers(300);
SELECT groupArrayMovingAvg(100)(toDecimal32(1, 9)) FROM numbers(300);

