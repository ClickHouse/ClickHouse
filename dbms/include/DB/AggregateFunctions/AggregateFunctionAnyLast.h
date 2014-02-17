#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionAnyLastData
{
	Field value;
};


/// Берёт последнее попавшееся значение
class AggregateFunctionAnyLast : public IUnaryAggregateFunction<AggregateFunctionAnyLastData, AggregateFunctionAnyLast>
{
private:
	DataTypePtr type;
	
public:
	String getName() const { return "anyLast"; }

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
		column.get(row_num, data(place).value);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		if (!data(rhs).value.isNull())
			data(place).value = data(rhs).value;
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		const Data & d = data(place);

		if (unlikely(d.value.isNull()))
		{
			writeBinary(false, buf);
		}
		else
		{
			writeBinary(true, buf);
			type->serializeBinary(data(place).value, buf);
		}
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		bool is_not_null = false;
		readBinary(is_not_null, buf);

		if (is_not_null)
			type->deserializeBinary(data(place).value, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		if (unlikely(data(place).value.isNull()))
			to.insertDefault();
		else
			to.insert(data(place).value);
	}
};

}
