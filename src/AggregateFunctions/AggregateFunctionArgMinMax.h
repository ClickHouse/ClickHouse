#pragma once

#include <base/StringRef.h>
#include <DataTypes/IDataType.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionMinMaxAny.h> // SingleValueDataString used in embedded compiler


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// For possible values for template parameters, see 'AggregateFunctionMinMaxAny.h'.
template <typename ResultData, typename ValueData>
struct AggregateFunctionArgMinMaxData
{
    using ResultData_t = ResultData;
    using ValueData_t = ValueData;

    ResultData result;  // the argument at which the minimum/maximum value is reached.
    ValueData value;    // value for which the minimum/maximum is calculated.

    static bool allocatesMemoryInArena()
    {
        return ResultData::allocatesMemoryInArena() || ValueData::allocatesMemoryInArena();
    }
};

/// Returns the first arg value found for the minimum/maximum value. Example: argMax(arg, value).
template <typename Data>
class AggregateFunctionArgMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>
{
private:
    const DataTypePtr & type_res;
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_)
        : Base({type_res_, type_val_}, {})
        , type_res(this->argument_types[0])
        , type_val(this->argument_types[1])
        , serialization_res(type_res->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
    {
        if (!type_val->isComparable())
            throw Exception("Illegal type " + type_val->getName() + " of second argument of aggregate function " + getName()
                + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return StringRef(Data::ValueData_t::name()) == StringRef("min") ? "argMin" : "argMax";
    }

    DataTypePtr getReturnType() const override
    {
        return type_res;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(*columns[1], row_num, arena))
            this->data(place).result.change(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(this->data(rhs).value, arena))
            this->data(place).result.change(this->data(rhs).result, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).result.write(buf, *serialization_res);
        this->data(place).value.write(buf, *serialization_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).result.read(buf, *serialization_res, arena);
        this->data(place).value.read(buf, *serialization_val, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return Data::allocatesMemoryInArena();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result.insertResultInto(to);
    }
};

}
