#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Types.h>
#include <Core/ValuesWithType.h>
#include <Interpreters/Context.h>

namespace DB
{
/** reverseBySeparator(string[, separator]) - reverses the order of parts in a string separated by a separator.
  * Returns a string with parts ordered from right to left, joined by the same separator.
  */
class FunctionReverseBySeparator final : public IFunction
{
public:
    static constexpr auto name = "reverseBySeparator";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReverseBySeparator>(); }

    [[nodiscard]] String getName() const override { return name; }

    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return true; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] String getSignatureString() const override
    {
        return "(String, [const String]) -> String";
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
