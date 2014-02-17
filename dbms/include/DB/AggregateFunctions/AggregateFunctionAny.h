#pragma once

#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>

#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


struct AggregateFunctionAnyData
{
	Field value;
};


/// Берёт первое попавшееся значение
class AggregateFunctionAny : public IUnaryAggregateFunction<AggregateFunctionAnyData, AggregateFunctionAny>
{
private:
	DataTypePtr type;
	
public:
	String getName() const { return "any"; }

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
		Data & d = data(place);

		if (!d.value.isNull())
			return;
		column.get(row_num, d.value);
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		Data & d = data(place);

		if (d.value.isNull())
			d.value = data(rhs).value;
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
		Data & d = data(place);

		bool is_not_null = false;
		readBinary(is_not_null, buf);

		if (is_not_null)
		{
			Field tmp;
			type->deserializeBinary(tmp, buf);

			if (d.value.isNull())
				d.value = tmp;
		}
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
