#pragma once

#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeCustom.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = VectorWithMemoryTracking<DataTypePtr>;

/** The type SimpleAggregateFunction(fct, type) is meant to be used in an AggregatingMergeTree. It behaves like a standard
 * data type but when rows are merged, an aggregation function is applied.
 *
 * The aggregation function is limited to simple functions whose merge state is the final result:
 * any, anyLast, min, max, sum
 *
 * Examples:
 *
 * SimpleAggregateFunction(sum, Nullable(Float64))
 * SimpleAggregateFunction(anyLast, LowCardinality(Nullable(String)))
 * SimpleAggregateFunction(anyLast, IPv4)
 *
 * Technically, a standard IDataType is instantiated and customized with IDataTypeCustomName and DataTypeCustomDesc.
  */

class DataTypeCustomSimpleAggregateFunction : public IDataTypeCustomName
{
private:
    const AggregateFunctionPtr function;
    const DataTypes argument_types;
    const Array parameters;

public:
    DataTypeCustomSimpleAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
            : function(function_), argument_types(argument_types_), parameters(parameters_) {}

    AggregateFunctionPtr getFunction() const { return function; }
    String getFunctionName() const;
    const DataTypes & getArgumentsDataTypes() const { return argument_types; }
    const Array & getParameters() const { return parameters; }
    String getName() const override;
    std::optional<Field> getDefault() const override;

    /// SimpleAggregateFunction values intentionally behave like their stored
    /// argument type. Propagate strict custom semantics from that argument instead
    /// of inventing a second semantic identity for the wrapper.
    std::optional<String> getSemanticIdentity() const override
    {
        if (argument_types.size() == 1)
            return getCustomTypeSemanticIdentity(argument_types[0]);
        return std::nullopt;
    }

    bool requiresValueValidation() const override
    {
        return argument_types.size() == 1 && containsCustomTypeValueValidation(argument_types[0]);
    }

    void validateColumn(const IColumn & column, const String & operation) const override
    {
        if (argument_types.size() == 1)
            validateCustomDataTypeColumn(column, argument_types[0], operation);
    }

    static void checkSupportedFunctions(const AggregateFunctionPtr & function);
};

DataTypePtr createSimpleAggregateFunctionType(const AggregateFunctionPtr & function, const DataTypes & argument_types, const Array & parameters);

}
