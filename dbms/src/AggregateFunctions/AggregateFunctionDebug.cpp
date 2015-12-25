#include <DB/AggregateFunctions/INullaryAggregateFunction.h>
#include <DB/AggregateFunctions/AggregateFunctionFactory.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Columns/ColumnsNumber.h>


namespace DB
{


/** Управление состояниями агрегатных функций делается нетривиальным образом:
  * - память для них нужно выделяется в пуле,
  *   указатели на эти состояния могут передаваться между различными структурами данных,
  *   при этом нельзя сделать RAII-обёртки для каждого отдельного состояния.
  * Подробнее см. Aggregator.h.
  *
  * В связи с этим, возникают трудно-отлаживаемые баги.
  * Для упрощения воспроизведения багов, была написана агрегатная функция debug,
  *  и её исходники решено не удалять после отладки.
  *
  * Эта агрегатная функция принимает ноль аргументов и ничего не делает.
  * Но у неё сделано состояние, которое нетривиально создаётся и уничтожается.
  */


struct AggregateFunctionDebugData
{
	std::unique_ptr<size_t> ptr { new size_t(0xABCDEF01DEADBEEF) };
	AggregateFunctionDebugData() {}
	~AggregateFunctionDebugData()
	{
		if (*ptr != 0xABCDEF01DEADBEEF)
		{
			std::cerr << "Bug!";
			abort();
		}

		ptr.reset();
	}
};


class AggregateFunctionDebug final : public INullaryAggregateFunction<AggregateFunctionDebugData, AggregateFunctionDebug>
{
public:
	String getName() const override { return "debug"; }

	DataTypePtr getReturnType() const override
	{
		return new DataTypeUInt8;
	}

	void addImpl(AggregateDataPtr place) const
	{
	}

	void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs) const override
	{
	}

	void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
	{
		writeBinary(UInt8(0), buf);
	}

	void deserializeMerge(AggregateDataPtr place, ReadBuffer & buf) const override
	{
		UInt8 tmp;
		readBinary(tmp, buf);
	}

	void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
	{
		static_cast<ColumnUInt8 &>(to).getData().push_back(0);
	}
};


AggregateFunctionPtr createAggregateFunctionDebug(const std::string & name, const DataTypes & argument_types)
{
	return new AggregateFunctionDebug;
}


void registerAggregateFunctionDebug(AggregateFunctionFactory & factory)
{
	factory.registerFunction({"debug"}, createAggregateFunctionDebug);
}

}
