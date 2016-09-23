#pragma once

#include <DB/AggregateFunctions/AggregateFunctionsMinMaxAny.h>
#include <DB/AggregateFunctions/IBinaryAggregateFunction.h>


namespace DB
{


/// Возможные значения параметров шаблонов см. в AggregateFunctionsMinMaxAny.h
template <typename ResultData, typename ValueData>
struct AggregateFunctionsArgMinMaxData
{
	using ResultData_t = ResultData;
	using ValueData_t = ValueData;

	ResultData result;	// аргумент, при котором достигается минимальное/максимальное значение value.
	ValueData value;	// значение, для которого считается минимум/максимум.
};

/// Возвращает первое попавшееся значение arg для минимального/максимального value. Пример: argMax(arg, value).
template <typename Data>
class AggregateFunctionsArgMinMax final : public IBinaryAggregateFunction<Data, AggregateFunctionsArgMinMax<Data>>
{
private:
	DataTypePtr type_res;
	DataTypePtr type_val;

public:
	String getName() const override { return (0 == strcmp(Data::ValueData_t::name(), "min")) ? "argMin" : "argMax"; }

	DataTypePtr getReturnType() const override
	{
		return type_res;
	}

	void setArgumentsImpl(const DataTypes & arguments)
	{
		type_res = arguments[0];
		type_val = arguments[1];
	}

	void addImpl(AggregateDataPtr place, const IColumn & column_arg, const IColumn & column_max, size_t row_num, Arena *) const
	{
		if (this->data(place).value.changeIfBetter(column_max, row_num))
			this->data(place).result.change(column_arg, row_num);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
	{
		if (this->data(place).value.changeIfBetter(this->data(rhs).value))
			this->data(place).result.change(this->data(rhs).result);
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		this->data(place).result.write(buf, *type_res.get());
		this->data(place).value.write(buf, *type_val.get());
	}

	void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
	{
		this->data(place).result.read(buf, *type_res.get());
		this->data(place).value.read(buf, *type_val.get());
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		this->data(place).result.insertResultInto(to);
	}
};

}
