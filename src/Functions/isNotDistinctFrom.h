#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/**
  * Performs null-safe comparison.
  * equals(NULL, NULL) is NULL, while isNotDistinctFrom(NULL, NULL) is true.
  * Currently, it can be used only in the JOIN ON section.
  * This wrapper is needed to register function to make possible query analysis, syntax completion and so on.
  */
class FunctionIsNotDistinctFrom : public IFunction
{
public:
    static constexpr auto name = "isNotDistinctFrom";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsNotDistinctFrom>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & /* arguments */, const DataTypePtr &, size_t /* rows_count */) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} can be used only in the JOIN ON section", getName());
    }
};

}
