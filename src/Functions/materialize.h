#pragma once
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/** materialize(x) - materialize the constant
  */
template <bool remove_special_representations>
class FunctionMaterialize final : public IFunction
{
public:
    static constexpr auto name = "materialize";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionMaterialize<remove_special_representations>>();
    }

    /// Get the function name.
    [[nodiscard]] String getName() const override
    {
        return name;
    }

    [[nodiscard]] bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override
    {
        return true;
    }

    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool isNullPropagating(const DataTypePtr & result_type) const override { return isNullableOrLowCardinalityNullable(result_type); }

    [[nodiscard]] bool useDefaultImplementationForNothing() const override { return false; }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return false; }

    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    [[nodiscard]] bool useDefaultImplementationForSparseColumns() const override { return false; }

    [[nodiscard]] bool isSuitableForConstantFolding() const override { return false; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    [[nodiscard]] size_t getNumberOfArguments() const override
    {
        return 1;
    }

    [[nodiscard]] String getSignatureString() const override { return "(T) -> T"; }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto res = arguments[0].column->convertToFullColumnIfConst();
        if constexpr (remove_special_representations)
            res = removeSpecialRepresentations(res);
        return res;
    }

    [[nodiscard]] bool hasInformationAboutMonotonicity() const override { return true; }

    [[nodiscard]] Monotonicity getMonotonicityForRange(const IDataType &, const Field &, const Field &) const override
    {
        /// Depending on the argument the function materialize() is either a constant or works as identity().
        /// In both cases this function preserves values, so it is strictly monotonic and non-decreasing.
        return {.is_monotonic = true, .is_always_monotonic = true, .is_strict = true};
    }
};

}
