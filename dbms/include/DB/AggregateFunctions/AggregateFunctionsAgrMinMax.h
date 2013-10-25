#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateFunctionAgrMinTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs < rhs; }
	static String name() { return "agrMin"; }
};

struct AggregateFunctionAgrMaxTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs > rhs; }
	static String name() { return "agrMax"; }
};


struct AggregateFunctionsAgrMinMaxData
{
	Field value;
	Field result;
};


/// Возвращает первое попавшееся значение arg для минимального/максимального value
template <typename Traits>
class AggregateFunctionsAgrMinMax : public IAggregateFunctionHelper<AggregateFunctionsAgrMinMaxData>
{
private:
	DataTypePtr typeRes;
	DataTypePtr typeVal;

public:

	String getName() const { return Traits::name(); }

	DataTypePtr getReturnType() const
	{
		return typeRes;
	}

	void setArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 2)
			throw Exception("Aggregate function " + getName() + " requires exactly two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		typeRes = arguments[0];
		typeVal = arguments[1];
	}

	void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num) const
	{
		Field value, result;
		columns[0]->get(row_num, result);
		columns[1]->get(row_num, value);
		Data & d = data(place);

		if (!d.value.isNull())
		{
			if (Traits::better(value, d.value)) {
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
			if (Traits::better(d_rhs.value, d.value)){
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
		typeVal->serializeBinary(data(place).value, buf);
		typeRes->serializeBinary(data(place).result, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data & d = data(place);

		if (!d.value.isNull())
		{
			Field value_, result_;
			typeRes->deserializeBinary(result_, buf);
			typeVal->deserializeBinary(value_, buf);
			if (Traits::better(value_, d.value)) {
				d.value = value_;
				d.result = result_;
			}
		} else {
			typeRes->deserializeBinary(d.result, buf);
			typeVal->deserializeBinary(d.value, buf);
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


typedef AggregateFunctionsAgrMinMax<AggregateFunctionAgrMinTraits> AggregateFunctionAgrMin;
typedef AggregateFunctionsAgrMinMax<AggregateFunctionAgrMaxTraits> AggregateFunctionAgrMax;

}
