#pragma once

#include <string>

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/IExternalLoadable.h>


namespace DB
{

struct UserDefinedAggregateFunctionConfiguration
{
    std::string name;
    FunctionPtr initialize_func;
    FunctionPtr process_func;
    FunctionPtr merge_func;
    FunctionPtr finalize_func;
    DataTypes argument_types, 
    Array parameters_, 
};

class UserDefinedAggreagteFunction final : public IExternalLoadable, IAggregateFunctionDataHelper<Field, UserDefinedAggreagteFunction>
{
public:

UserDefinedAggreagteFunction(
        const UserDefinedAggregateFunctionConfiguration & configuration_,
        const ExternalLoadableLifetime & lifetime_) 
        : configuration(configuration_), lifetime(lifetime_), state_type(configuration_.initialize_func->getReturnTypeImpl(DataTypes())),
          IAggregateFunctionHelper(
            aconfiguration_.rgument_types_,
            configuration_.parameters_,
            configuration_.finalize_func->getReturnTypeImpl(configuration_.initialize_func->getReturnTypeImpl(DataTypes()))
        ) {}


    const ExternalLoadableLifetime & getLifetime() const override
    {
        return lifetime;
    }

    std::string getLoadableName() const override
    {
        return configuration.name;
    }

    String getName() const override {
        return configuration.name;
    }

    bool supportUpdates() const override
    {
        return true;
    }

    bool isModified() const override
    {
        return true;
    }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<UserDefinedAggregateFunctionConfiguration>(configuration, lifetime);
    }

    const UserDefinedAggregateFunctionConfiguration & getConfiguration() const
    {
        return configuration;
    }

    std::shared_ptr<UserDefinedAggreagteFunction> shared_from_this()
    {
        return std::static_pointer_cast<UserDefinedAggreagteFunction>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const UserDefinedAggreagteFunction> shared_from_this() const
    {
        return std::static_pointer_cast<const UserDefinedAggreagteFunction>(IExternalLoadable::shared_from_this());
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Field(configuration.initialize_func->executeImpl(ColumnsWithTypeAndName(), state_type, 0));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override;

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override;

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version = std::nullopt) const override;

    void deserialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version = std::nullopt) const override;

private:
    UserDefinedAggregateFunctionConfiguration configuration;
    ExternalLoadableLifetime lifetime;
    DataTypePtr state_type;
};

}
