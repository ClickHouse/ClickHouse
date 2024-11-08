#pragma once

#include <Columns/ColumnFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <DataTypes/DataTypeFixedString.h>

namespace DB
{

class FunctionApproximateL2Distance : public IFunction
{
public:
    static constexpr auto name = "approximateL2Distance";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionApproximateL2Distance>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
