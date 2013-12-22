#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionMinTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs < rhs; }
	static String name() { return "min"; }
};

struct AggregateFunctionMaxTraits
{
	static bool better(const Field & lhs, const Field & rhs) { return lhs > rhs; }
	static String name() { return "max"; }
};


struct AggregateFunctionsMinMaxData
{
	Field value;
};


/// Берёт минимальное (или максимальное) значение. Если таких много - то первое попавшееся из них.
template <typename Traits>
class AggregateFunctionsMinMax : public IUnaryAggregateFunction<AggregateFunctionsMinMaxData, AggregateFunctionsMinMax<Traits> >
{
private:
	typedef typename IAggregateFunctionHelper<AggregateFunctionsMinMaxData>::Data Data;
	DataTypePtr type;
	
public:
	String getName() const { return Traits::name(); }

	DataTypePtr getReturnType() const
	{
		return type;
	}

	void setArgument(const DataTypePtr & argument)
	{
		type = argument;
	}


	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		Field value;
		column.get(row_num, value);
		Data & d = this->data(place);

		if (!d.value.isNull())
		{
			if (Traits::better(value, d.value))
				d.value = value;
		}
		else
			d.value = value;
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		Data & d = this->data(place);
		const Data & d_rhs = this->data(rhs);
		
		if (!d.value.isNull())
		{
			if (Traits::better(d_rhs.value, d.value))
				d.value = d_rhs.value;
		}
		else
			d.value = d_rhs.value;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		type->serializeBinary(this->data(place).value, buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		Data & d = this->data(place);
		
		if (!d.value.isNull())
		{
			Field value_;
			type->deserializeBinary(value_, buf);
			if (Traits::better(value_, d.value))
				d.value = value_;
		}
		else
			type->deserializeBinary(d.value, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		if (unlikely(this->data(place).value.isNull()))
			to.insertDefault();
		else
			to.insert(this->data(place).value);
	}
};


typedef AggregateFunctionsMinMax<AggregateFunctionMinTraits> AggregateFunctionMin;
typedef AggregateFunctionsMinMax<AggregateFunctionMaxTraits> AggregateFunctionMax;

}
