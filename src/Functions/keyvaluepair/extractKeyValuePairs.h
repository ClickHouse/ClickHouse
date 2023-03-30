#pragma once

#include <Columns/ColumnsNumber.h>

#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

namespace DB
{

class ExtractKeyValuePairs : public IFunction
{
public:
    ExtractKeyValuePairs();

    static constexpr auto name = "extractKeyValuePairs";

    String getName() const override
    {
        return name;
    }

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ExtractKeyValuePairs>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override;
    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;

    bool isVariadic() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

    std::size_t getNumberOfArguments() const override
    {
        return 0u;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        return {1, 2, 3, 4, 5};
    }
};

}
