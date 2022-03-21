#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{
struct Settings;

/** Not an aggregate function, but an adapter of aggregate functions.
  * Aggregate functions with the `SimpleState` suffix is almost identical to the corresponding ones,
  * except the return type becomes DataTypeCustomSimpleAggregateFunction.
  */
class AggregateFunctionSimpleState final : public IAggregateFunctionHelper<AggregateFunctionSimpleState>
{
private:
    AggregateFunctionPtr nested_func;

public:
    AggregateFunctionSimpleState(AggregateFunctionPtr nested_, const DataTypes & arguments_, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionSimpleState>(arguments_, params_)
        , nested_func(nested_)
    {
    }

    String getName() const override { return nested_func->getName() + "SimpleState"; }

    DataTypePtr getReturnType() const override
    {
        DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(nested_func);

        // Need to make a clone to avoid recursive reference.
        auto storage_type_out = DataTypeFactory::instance().get(nested_func->getReturnType()->getName());
        // Need to make a new function with promoted argument types because SimpleAggregates requires arg_type = return_type.
        AggregateFunctionProperties properties;
        auto function
            = AggregateFunctionFactory::instance().get(nested_func->getName(), {storage_type_out}, nested_func->getParameters(), properties);

        // Need to make a clone because it'll be customized.
        auto storage_type_arg = DataTypeFactory::instance().get(nested_func->getReturnType()->getName());
        DataTypeCustomNamePtr custom_name
            = std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, DataTypes{nested_func->getReturnType()}, parameters);
        storage_type_arg->setCustomization(std::make_unique<DataTypeCustomDesc>(std::move(custom_name), nullptr));
        return storage_type_arg;
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    void create(AggregateDataPtr __restrict place) const override { nested_func->create(place); }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_func->destroy(place); }

    bool hasTrivialDestructor() const override { return nested_func->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return nested_func->sizeOfData(); }

    size_t alignOfData() const override { return nested_func->alignOfData(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        nested_func->add(place, columns, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override { return nested_func->allocatesMemoryInArena(); }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
