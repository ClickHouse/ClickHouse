#include <AggregateFunctions/INullaryAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{


/** State management of aggregate functions is done in a non-trivial way:
  * - the memory for them needs to be allocated in the pool,
  *   pointers to these states can be passed between different data structures,
  *   herewith, you can not make RAII wrappers for each individual state.
  * For more information, see Aggregator.h.
  *
  * In this regard, there are difficult-debugging bugs.
  * To simplify the playback of bugs, an aggregate `debug` function was written,
  *  and its source code is decided not to delete after debugging.
  *
  * This aggregate function takes zero arguments and does nothing.
  * But it has a state that is non-trivially created and destroyed.
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
        return std::make_shared<DataTypeUInt8>();
    }

    void addImpl(AggregateDataPtr place) const
    {
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(UInt8(0), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        UInt8 tmp;
        readBinary(tmp, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt8 &>(to).getData().push_back(0);
    }
};


AggregateFunctionPtr createAggregateFunctionDebug(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return std::make_shared<AggregateFunctionDebug>();
}


void registerAggregateFunctionDebug(AggregateFunctionFactory & factory)
{
    factory.registerFunction("debug", createAggregateFunctionDebug);
}

}
