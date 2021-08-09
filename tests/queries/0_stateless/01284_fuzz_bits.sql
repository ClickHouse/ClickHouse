SELECT fuzzBits(toString('string'), 1); -- { serverError 43 }
SELECT fuzzBits('string', -1.0); -- { serverError 69 }
SELECT fuzzBits('', 0.3);
SELECT length(fuzzBits(randomString(100), 0.5));
SELECT toTypeName(fuzzBits(randomString(100), 0.5));
SELECT toTypeName(fuzzBits(toFixedString('abacaba', 10), 0.9));

SELECT
  (
    0.29 * 8 * 10000 < sum
    AND sum < 0.31 * 8 * 10000
  ) AS res
FROM
  (
    SELECT
      arraySum(
        id -> bitCount(
          reinterpretAsUInt8(
            substring(
              fuzzBits(
                arrayStringConcat(arrayMap(x -> toString('\0'), range(10000))),
                0.3
              ),
              id + 1,
              1
            )
          )
        ),
        range(10000)
      ) as sum
  )
