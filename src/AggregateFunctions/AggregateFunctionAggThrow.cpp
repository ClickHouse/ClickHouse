#include <memory>
#include <random>

#include <DataTypes/DataTypesNumber.h>
#include <Common/thread_local_rng.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int AGGREGATE_FUNCTION_THROW;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct AggregateFunctionThrowData
{
    bool allocated;

    AggregateFunctionThrowData() : allocated(true) {}
    ~AggregateFunctionThrowData()
    {
        volatile bool * allocated_ptr = &allocated;

        if (*allocated_ptr)
            *allocated_ptr = false;
        else
            abort();
    }
};

/** Throw on creation with probability specified in parameter.
  * It will check correct destruction of the state.
  * This is intended to check for exception safety.
  */
class AggregateFunctionThrow final : public IAggregateFunctionDataHelper<AggregateFunctionThrowData, AggregateFunctionThrow>
{
private:
    Float64 throw_probability;

public:
    AggregateFunctionThrow(const DataTypes & argument_types_, const Array & parameters_, Float64 throw_probability_)
        : IAggregateFunctionDataHelper(argument_types_, parameters_), throw_probability(throw_probability_) {}

    String getName() const override
    {
        return "aggThrow";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override
    {
        if (std::uniform_real_distribution<>(0.0, 1.0)(thread_local_rng) <= throw_probability)
            throw Exception("Aggregate function " + getName() + " has thrown exception successfully", ErrorCodes::AGGREGATE_FUNCTION_THROW);

        new (place) Data;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override
    {
    }

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena *) const override
    {
    }

    void serialize(ConstAggregateDataPtr, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        char c = 0;
        buf.write(c);
    }

    void deserialize(AggregateDataPtr /* place */, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        char c = 0;
        buf.read(c);
    }

    void insertResultInto(AggregateDataPtr, IColumn & to, Arena *) const override
    {
        to.insertDefault();
    }
};

}

void registerAggregateFunctionAggThrow(AggregateFunctionFactory & factory)
{
    factory.registerFunction("aggThrow", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        Float64 throw_probability = 1.0;
        if (parameters.size() == 1)
            throw_probability = parameters[0].safeGet<Float64>();
        else if (parameters.size() > 1)
            throw Exception("Aggregate function " + name + " cannot have more than one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return std::make_shared<AggregateFunctionThrow>(argument_types, parameters, throw_probability);
    });
}

}
