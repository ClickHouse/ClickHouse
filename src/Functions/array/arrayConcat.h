#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// arrayConcat(arr1, ...) - concatenate arrays.
class FunctionArrayConcat final : public IFunction
{
public:
    static constexpr auto name = "arrayConcat";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayConcat>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(T : Array, ...) -> leastSupertype(T, ...)";
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
};

}
