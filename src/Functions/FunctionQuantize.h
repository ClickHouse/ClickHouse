// FunctionQuantize.h

#pragma once

#include <memory>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Common/Logger.h>
#include "Interpreters/Context_fwd.h"

namespace DB
{

class FunctionQuantize : public IFunction
{
public:
    static constexpr auto name = "quantize";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionQuantize>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
