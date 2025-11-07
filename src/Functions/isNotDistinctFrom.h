#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

template <typename A, typename B>
struct IsNotDistinctFromOp
{
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = IsNotDistinctFromOp<B, A>;

    static UInt8 apply(A a, B b) { return EqualsOp<A, B>::apply(a, b); }
};

struct NameFunctionIsNotDistinctFrom
{
    static constexpr auto name = "isNotDistinctFrom";
};

/**
  * Performs null-safe comparison.
  * equals(NULL, NULL) is NULL, while isNotDistinctFrom(NULL, NULL) is true.
  * Currently, it can be used only in the JOIN ON section.
  * This wrapper is needed to register function to make possible query analysis, syntax completion and so on.
  */
class FunctionIsNotDistinctFrom : public IFunction
{
public:
    static constexpr auto name = NameFunctionIsNotDistinctFrom::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionIsNotDistinctFrom>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    } // because we return false here, the result_type will be not nullable

    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return std::make_shared<DataTypeUInt8>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t rows_count) const override
    {
        ComparisonParams params;
        using FunctionIsNotDistinctFromImpl = FunctionComparison<IsNotDistinctFromOp, NameFunctionIsNotDistinctFrom>;
        FunctionIsNotDistinctFromImpl func_is_not_distinct_from(params);
        return func_is_not_distinct_from.executeImpl(arguments, result_type, rows_count);
    }
};

}
