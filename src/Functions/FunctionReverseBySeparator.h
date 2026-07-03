#pragma once

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Core/Types.h>
#include <Core/ValuesWithType.h>
#include <Interpreters/Context.h>

namespace DB
{
/** reverseBySeparator(string[, separator]) - reverses the order of parts in a string separated by a separator.
  * Returns a string with parts ordered from right to left, joined by the same separator.
  */
class FunctionReverseBySeparator : public IFunction
{
public:
    static constexpr auto name = "reverseBySeparator";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionReverseBySeparator>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};

}
