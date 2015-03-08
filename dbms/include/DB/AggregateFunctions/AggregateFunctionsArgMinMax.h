#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/AggregateFunctionsMinMaxAny.h>


namespace DB
{


/// Возможные значения параметров шаблонов см. в AggregateFunctionsMinMaxAny.h
template <typename ResultData, typename ValueData>
struct AggregateFunctionsArgMinMaxData
{
	ResultData result;	// аргумент, при котором достигается минимальное/максимальное значение value.
	ValueData value;	// значение, для которого считается минимум/максимум.
};

/// Возвращает первое попавшееся значение arg для минимального/максимального value. Пример: argMax(arg, value).
template <typename Data>
class AggregateFunctionsArgMinMax final : public IAggregateFunctionHelper<Data>
{
private:
	DataTypePtr type_res;
	DataTypePtr type_val;

public:
	String getName() const { return (0 == strcmp(decltype(Data::value)::name(), "min")) ? "argMin" : "argMax"; }

	DataTypePtr getReturnType() const
	{
		return type_res;
	}

	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 2)
			throw Exception("Aggregate function " + getName() + " requires exactly two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		type_res = arguments[0];
		type_val = arguments[1];
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		if (this->data(place).value.changeIfBetter(*columns[1], row_num))
			this->data(place).result.change(*columns[0], row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		if (this->data(place).value.changeIfBetter(this->data(rhs).value))
			this->data(place).result.change(this->data(rhs).result);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		this->data(place).result.write(buf, *type_res.get());
		this->data(place).value.write(buf, *type_val.get());
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data rhs;	/// Для строчек не очень оптимально, так как может делаться одна лишняя аллокация.

		rhs.result.read(buf, *type_res.get());
		rhs.value.read(buf, *type_val.get());

		if (this->data(place).value.changeIfBetter(rhs.value))
			this->data(place).result.change(rhs.result);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		this->data(place).result.insertResultInto(to);
	}
};

}
