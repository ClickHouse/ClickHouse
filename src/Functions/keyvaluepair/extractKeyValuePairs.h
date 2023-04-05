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

    String getName() const override;

    static FunctionPtr create(ContextPtr);

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t) const override;

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;

    bool isVariadic() const override;

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override;

    std::size_t getNumberOfArguments() const override;

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override;
};

}
