-- https://github.com/ClickHouse/ClickHouse/issues/55655
select *
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
	)
format Null;
-- It should not throw exception and the output is aggregation function state which is binary.
