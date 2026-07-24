#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>

namespace DB
{

/** materialize(x) - materialize the constant
  */
template <bool remove_sparse>
class FunctionMaterialize : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMaterialize<remove_sparse>>();
    }

    /// Get the function name.
    String getName() const override
    {
        return name;
    }

    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override
    {
        return true;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool useDefaultImplementationForNothing() const override { return false; }

    bool useDefaultImplementationForConstants() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool useDefaultImplementationForSparseColumns() const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto res = arguments[0].column->convertToFullColumnIfConst();
        if constexpr (remove_sparse)
            res = recursiveRemoveSparse(res);
        return res;
    }

    bool hasInformationAboutMonotonicity() const override { return true; }

    Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        /// Depending on the argument the function materialize() is either a constant or works as identity().
        /// In both cases this function is monotonic and non-decreasing.
        return {.is_monotonic = true, .is_always_monotonic = true};
    }
};

}
