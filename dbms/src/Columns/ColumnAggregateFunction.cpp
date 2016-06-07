#include <DB/AggregateFunctions/AggregateFunctionState.h>
#include <DB/Columns/ColumnAggregateFunction.h>


namespace DB
{

ColumnPtr ColumnAggregateFunction::convertToValues() const
{
	const IAggregateFunction * function = holder->func;
	ColumnPtr res = function->getReturnType()->createColumn();

	/** Если агрегатная функция возвращает нефинализированное состояние,
		* то надо просто скопировать указатели на него а также разделяемое владение данными.
		*
		* Также заменяем агрегатную функцию на вложенную.
		* То есть, если этот столбец - состояния агрегатной функции aggState,
		* то мы возвращаем такой же столбец, но с состояниями агрегатной функции agg.
		* Это одни и те же состояния, меняем только функцию, которой они соответствуют.
		*
		* Это довольно сложно для понимания.
		* Пример, когда такое происходит:
		*
		* SELECT k, finalizeAggregation(quantileTimingState(0.5)(x)) FROM ... GROUP BY k WITH TOTALS
		*
		* Здесь вычисляется агрегатная функция quantileTimingState.
		* Её тип возвращаемого значения:
		*  AggregateFunction(quantileTiming(0.5), UInt64).
		* Из-за наличия WITH TOTALS, при агрегации будут сохранены состояния этой агрегатной функции
		*  в столбце ColumnAggregateFunction, имеющего тип
		*  AggregateFunction(quantileTimingState(0.5), UInt64).
		* Затем в TotalsHavingBlockInputStream у него будет вызван метод convertToValues,
		*  чтобы получить "готовые" значения.
		* Но он всего лишь преобразует столбец типа
		*   AggregateFunction(quantileTimingState(0.5), UInt64)
		* в AggregateFunction(quantileTiming(0.5), UInt64)
		* - в такие же состояния.
		*
		* Затем будет вычислена функция finalizeAggregation, которая позовёт convertToValues уже от результата.
		* И это преобразует столбец типа
		*   AggregateFunction(quantileTiming(0.5), UInt64)
		* в UInt16 - уже готовый результат работы quantileTiming.
		*/
	if (const AggregateFunctionState * function_state = typeid_cast<const AggregateFunctionState *>(function))
	{
		ColumnAggregateFunction * res_ = new ColumnAggregateFunction(*this);
		res = res_;
		res_->set(function_state->getNestedFunction());
		res_->getData().assign(getData().begin(), getData().end());
		return res;
	}

	IColumn & column = *res;
	res->reserve(getData().size());

	for (auto val : getData())
		function->insertResultInto(val, column);

	return res;
}

}
