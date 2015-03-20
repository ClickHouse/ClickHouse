#include <DB/AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


/** Сделано в целях отладки. Подлежит удалению.
  */

struct AggregateFunctionDebugData
{
	UInt32 value;

	AggregateFunctionDebugData()
	{
		value = 0xAAAAAAAA;

		if (rand() % 1000 == 0)
			throw Exception("Test1");
	}

	~AggregateFunctionDebugData()
	{
		try
		{
			if (value == 0xDEADDEAD)
				throw Exception("Double free");

			if (value != 0xAAAAAAAA)
				throw Exception("Corruption");
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
			std::terminate();
		}

		value = 0xDEADDEAD;
	}
};

class AggregateFunctionDebug final : public IUnaryAggregateFunction<AggregateFunctionDebugData, AggregateFunctionDebug>
{
public:
	String getName() const { return "debug"; }

	DataTypePtr getReturnType() const
	{
		return new DataTypeUInt32;
	}

	void setArgument(const DataTypePtr & argument)
	{
	}

	void addOne(AggregateDataPtr place, const IColumn & column, size_t row_num) const
	{
		if (rand() % 1000 == 0)
			throw Exception("Test2");
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const
	{
		if (rand() % 1000 == 0)
			throw Exception("Test3");
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const
	{
		if (rand() % 1000 == 0)
			throw Exception("Test4");
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const
	{
		if (rand() % 1000 == 0)
			throw Exception("Test5");
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const
	{
		if (rand() % 1000 == 0)
			throw Exception("Test6");

		static_cast<ColumnUInt32 &>(to).getData().push_back(123);
	}
};

}
