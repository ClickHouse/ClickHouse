#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
/// Function may accept more then two arguments. If so, the first array with non-empty offsets is chosen.
class FunctionReplicate final : public IFunction
{
public:
    static constexpr auto name = "replicate";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionReplicate>();
    }

    [[nodiscard]] String getName() const override
    {
        return name;
    }

    [[nodiscard]] size_t getNumberOfArguments() const override
    {
        return 0;
    }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    [[nodiscard]] bool isVariadic() const override { return true; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    [[nodiscard]] bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(T, Array, ...) -> Array(T)";
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override;
};

}
