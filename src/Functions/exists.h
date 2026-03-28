#pragma once

#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/// This is a helper function for EXISTS expression.
/// It's not supposed to be ever executed, because it's argument is a subquery
/// and the whole EXISTS expression is either rewritten to '1 IN (SELECT 1 FROM <subquery>)'
/// if subquery is not correlated or it's replaced with JOINs during decorrelation.
class FunctionExists : public IFunction
{
public:
    String getName() const override { return "exists"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /*arguments*/, const DataTypePtr & /*result_type*/, size_t  /*input_rows_count*/) const override
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function 'exists' is not supposed to be executed");
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }
};

}
