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
        : IAggregateFunctionDataHelper<ExponentiallySmoothedAverage, AggregateFunctionExponentialMovingAverage>(argument_types_, params)
    {
        if (params.size() != 1)
            throw Exception{"Aggregate function " + getName() + " requires exactly one parameter: half decay time.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        half_decay = applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
    }

    String getName() const override
    {
        return "exponentialMovingAverage";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<Float64>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & value = columns[0]->getFloat64(row_num);
        const auto & time = columns[1]->getFloat64(row_num);
        this->data(place).add(value, time, half_decay);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), half_decay);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).value, buf);
        writeBinary(this->data(place).time, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
        readBinary(this->data(place).time, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column = assert_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(this->data(place).get(half_decay));
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
