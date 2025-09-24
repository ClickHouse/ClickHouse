#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>
#include <Functions/FunctionsComparison.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct NameFunctionIsDistinctFrom { static constexpr auto name = "isDistinctFrom"; };

/**
  * Performs null-safe comparison.
  */
class FunctionIsDistinctFrom : public IFunction
{
private:
    const ComparisonParams params;
public:

    static constexpr auto name = NameFunctionIsDistinctFrom::name;
    explicit FunctionIsDistinctFrom(ComparisonParams params_) : params(std::move(params_)) {}

    static FunctionPtr create(ContextPtr context)
    {
      return std::make_shared<FunctionIsDistinctFrom>(context ? ComparisonParams(context) : ComparisonParams());
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
};

}
