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
	Field result;	// аргумент, при котором достигается минимальное/максимальное значение value.
	Field value;	// значение, для которого считается минимум/максимум.
};

/// Возвращает первое попавшееся значение arg для минимального/максимального value. Пример: argMax(arg, value).
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
		Field result;
		Field value;
		columns[0]->get(row_num, result);
		columns[1]->get(row_num, value);
		Data & d = data(place);

		if (!d.value.isNull())
		{
			if (Traits::better(value, d.value))
			{
				d.result = result;
				d.value = value;
			}
		}
		else
		{
			d.result = result;
			d.value = value;
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
				d.result = d_rhs.result;
				d.value = d_rhs.value;
			}
		}
		else
		{
			d.result = d_rhs.result;
			d.value = d_rhs.value;
		}
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		const Data & d = data(place);

		if (unlikely(d.result.isNull()))
		{
			writeBinary(false, buf);
		}
		else
		{
			writeBinary(true, buf);
			type_res->serializeBinary(d.result, buf);
			type_val->serializeBinary(d.value, buf);
		}
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data & d = data(place);

		bool is_not_null = false;
		readBinary(is_not_null, buf);

		if (is_not_null)
		{
			if (!d.value.isNull())
			{
				Field result_;
				Field value_;

				type_res->deserializeBinary(result_, buf);
				type_val->deserializeBinary(value_, buf);

				if (Traits::better(value_, d.value))
				{
					d.result = result_;
					d.value = value_;
				}
			}
			else
			{
				type_res->deserializeBinary(d.result, buf);
				type_val->deserializeBinary(d.value, buf);
			}
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
