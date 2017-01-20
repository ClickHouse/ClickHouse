#include <DB/AggregateFunctions/AggregateFunctionState.h>
#include <DB/Columns/ColumnAggregateFunction.h>


namespace DB
{

ColumnPtr ColumnAggregateFunction::convertToValues() const
{
	const IAggregateFunction * function = func.get();
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
		std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);
		res->set(function_state->getNestedFunction());
		res->getData().assign(getData().begin(), getData().end());
		return res;
	}

	IColumn & column = *res;
	res->reserve(getData().size());

	for (auto val : getData())
		function->insertResultInto(val, column);

	return res;
}


void ColumnAggregateFunction::insertRangeFrom(const IColumn & from, size_t start, size_t length)
{
	const ColumnAggregateFunction & from_concrete = static_cast<const ColumnAggregateFunction &>(from);

	if (start + length > from_concrete.getData().size())
		throw Exception("Parameters start = "
			+ toString(start) + ", length = "
			+ toString(length) + " are out of bound in ColumnAggregateFunction::insertRangeFrom method"
			" (data.size() = " + toString(from_concrete.getData().size()) + ").",
			ErrorCodes::PARAMETER_OUT_OF_BOUND);

	if (!empty() && src.get() != &from_concrete)
	{
		/// Must create new states of aggregate function and take ownership of it,
		///  because ownership of states of aggregate function cannot be shared for individual rows,
		///  (only as a whole).

		size_t end = start + length;
		for (size_t i = start; i < end; ++i)
			insertFrom(from, i);
	}
	else
	{
		/// Keep shared ownership of aggregation states.
		src = from_concrete.shared_from_this();

		auto & data = getData();
		size_t old_size = data.size();
		data.resize(old_size + length);
		memcpy(&data[old_size], &from_concrete.getData()[start], length * sizeof(data[0]));
	}
}


ColumnPtr ColumnAggregateFunction::filter(const Filter & filter, ssize_t result_size_hint) const
{
	size_t size = getData().size();
	if (size != filter.size())
		throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);

	if (size == 0)
		return res;

	auto & res_data = res->getData();

	if (result_size_hint)
		res_data.reserve(result_size_hint > 0 ? result_size_hint : size);

	for (size_t i = 0; i < size; ++i)
		if (filter[i])
			res_data.push_back(getData()[i]);

	/// Для экономии оперативки в случае слишком сильной фильтрации.
	if (res_data.size() * 2 < res_data.capacity())
		res_data = Container_t(res_data.cbegin(), res_data.cend());

	return res;
}


ColumnPtr ColumnAggregateFunction::permute(const Permutation & perm, size_t limit) const
{
	size_t size = getData().size();

	if (limit == 0)
		limit = size;
	else
		limit = std::min(size, limit);

	if (perm.size() < limit)
		throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

	std::shared_ptr<ColumnAggregateFunction> res = std::make_shared<ColumnAggregateFunction>(*this);

	res->getData().resize(limit);
	for (size_t i = 0; i < limit; ++i)
		res->getData()[i] = getData()[perm[i]];

	return res;
}

}
