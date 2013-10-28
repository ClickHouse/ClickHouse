#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{


struct AggregateFunctionArgMinTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs < rhs; }
	static String name() { return "argMin"; }
};

struct AggregateFunctionArgMaxTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs > rhs; }
	static String name() { return "argMax"; }
};

struct AggregateFunctionsArgMinMaxData
{
	Field value;
	Field result;
};

/// Возвращает первое попавшееся значение arg для минимального/максимального value
template <typename Traits>
class AggregateFunctionsArgMinMax : public IAggregateFunctionHelper<AggregateFunctionsArgMinMaxData>
{
private:
	DataTypePtr type_res;
	DataTypePtr type_val;

public:
	String getName() const { return Traits::name(); }

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
		Field value, result;
		columns[0]->get(row_num, result);
		columns[1]->get(row_num, value);
		Data & d = data(place);

		if (!d.value.isNull())
		{
			if (Traits::better(value, d.value))
			{
				d.value = value;
				d.result = result;
			}
		} else {
			d.value = value;
			d.result = result;
		}
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		Data & d = data(place);
		const Data & d_rhs = data(rhs);

		if (!d.value.isNull())
		{
			if (Traits::better(d_rhs.value, d.value))
			{
				d.value = d_rhs.value;
				d.result = d_rhs.result;
			}
		} else {
			d.value = d_rhs.value;
			d.result = d_rhs.result;
		}
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		type_val->serializeBinary(data(place).value, buf);
		type_res->serializeBinary(data(place).result, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data & d = data(place);

		if (!d.value.isNull())
		{
			Field value_, result_;
			type_res->deserializeBinary(result_, buf);
			type_val->deserializeBinary(value_, buf);
			if (Traits::better(value_, d.value))
			{
				d.value = value_;
				d.result = result_;
			}
		} else {
			type_res->deserializeBinary(d.result, buf);
			type_val->deserializeBinary(d.value, buf);
		}
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		if (unlikely(data(place).value.isNull()))
			to.insertDefault();
		else
			to.insert(data(place).result);
	}
};

typedef AggregateFunctionsArgMinMax<AggregateFunctionArgMinTraits> AggregateFunctionArgMin;
typedef AggregateFunctionsArgMinMax<AggregateFunctionArgMaxTraits> AggregateFunctionArgMax;


}
