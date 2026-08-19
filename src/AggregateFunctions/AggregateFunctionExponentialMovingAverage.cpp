#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/ExponentiallySmoothedCounter.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/** See the comments in ExponentiallySmoothedCounter.h
  */
class AggregateFunctionExponentialMovingAverage final
    : public IAggregateFunctionDataHelper<ExponentiallySmoothedAverage, AggregateFunctionExponentialMovingAverage>
{
private:
    String name;
    Float64 half_decay;

public:
    AggregateFunctionExponentialMovingAverage(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<ExponentiallySmoothedAverage, AggregateFunctionExponentialMovingAverage>(argument_types_, params, createResultType())
    {
        if (params.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one parameter: "
                "half decay time.", getName());

        half_decay = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
    }

    String getName() const override
    {
        return "exponentialMovingAverage";
    }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        const auto & time = columns[1]->getFloat64(row_num);
        data(place).add(value, time, half_decay);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).merge(data(rhs), half_decay);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(data(place).value, buf);
        writeBinary(data(place).time, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(data(place).value, buf);
        readBinary(data(place).time, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column = assert_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(data(place).get(half_decay));
    }
};

void registerAggregateFunctionExponentialMovingAverage(AggregateFunctionFactory & factory)
{
    factory.registerFunction("exponentialMovingAverage",
        [](const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *) -> AggregateFunctionPtr
        {
            assertBinary(name, argument_types);
            for (const auto & type : argument_types)
                if (!isNumber(*type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Both arguments for aggregate function {} must have numeric type, got {}", name, type->getName());
            return std::make_shared<AggregateFunctionExponentialMovingAverage>(argument_types, params);
        });
}

}
