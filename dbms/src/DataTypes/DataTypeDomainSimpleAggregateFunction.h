#pragma once

#include <DataTypes/IDataTypeDomain.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitors.h>

#include <IO/ReadHelpers.h>

namespace DB
{

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
 * Technically, a standard IDataType is instanciated and a DataTypeDomainSimpleAggregateFunction is added as domain.
  */

class DataTypeDomainSimpleAggregateFunction : public IDataTypeDomain
{
private:
    const AggregateFunctionPtr function;
    const DataTypes argument_types;
    const Array parameters;

public:
    DataTypeDomainSimpleAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
            : function(function_), argument_types(argument_types_), parameters(parameters_) {}

    const AggregateFunctionPtr getFunction() const { return function; }
    String doGetName() const override;
};

/// recursively follow data type domain to find a DataTypeDomainSimpleAggregateFunction
const DataTypeDomainSimpleAggregateFunction * findSimpleAggregateFunction(DataTypePtr dataType);

}
