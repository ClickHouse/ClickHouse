SET enable_analyzer = 1;

select * APPLY hex
from (
  select (
    select stochasticLogisticRegressionState(0.1, 0., 5, 'SGD')(number, number)
    from numbers(10)
  ) as col1,
  (
    select stochasticLinearRegressionState(0.1, 0., 5, 'SGD')(number, number)
    from numbers(10)
  ) as col2
from numbers(1)
);

SELECT *
FROM
(
    SELECT
        bitmapHasAny(bitmapBuild([toUInt8(1)]),
        (
            SELECT groupBitmapState(toUInt8(1))
        )) has1,
        bitmapHasAny(bitmapBuild([toUInt64(1)]),
        (
            SELECT groupBitmapState(toUInt64(2))
        )) has2
);
