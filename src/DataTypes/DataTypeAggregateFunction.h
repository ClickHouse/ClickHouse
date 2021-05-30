#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/IDataType.h>
#include <common/logger_useful.h>


namespace DB
{

/** Type - the state of the aggregate function.
  * Type parameters is an aggregate function, the types of its arguments, and its parameters (for parametric aggregate functions).
  */
class DataTypeAggregateFunction final : public IDataType
{
private:
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array parameters;
    mutable size_t version;

public:
    static constexpr bool is_parametric = true;

    DataTypeAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_,
                              const Array & parameters_, std::optional<size_t> version_ = std::nullopt)
        : function(function_)
        , argument_types(argument_types_)
        , parameters(parameters_)
        , version(version_ ? *version_ : function_->getDefaultVersion())
    {
    }

    String getFunctionName() const { return function->getName(); }
    AggregateFunctionPtr getFunction() const { return function; }

    String doGetName() const override;
    const char * getFamilyName() const override { return "AggregateFunction"; }
    TypeIndex getTypeId() const override { return TypeIndex::AggregateFunction; }

    bool canBeInsideNullable() const override { return false; }

    DataTypePtr getReturnType() const { return function->getReturnType(); }
    DataTypePtr getReturnTypeToPredict() const { return function->getReturnTypeToPredict(); }
    DataTypes getArgumentsDataTypes() const { return argument_types; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return false; }
    bool shouldAlignRightInPrettyFormats() const override { return false; }

    SerializationPtr doGetDefaultSerialization() const override;

    bool isVersioned() const { return function->isVersioned(); }
    void setVersion(size_t version_) const { version = version_; }
};

}
