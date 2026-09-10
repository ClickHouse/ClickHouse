#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/** arrayRemove(arr, elem)
  * Removes all elements equal to `elem` from `arr`.
  * NULLs are considered equal (so NULLs can be removed).
  *
  * Examples:
  *   arrayRemove([1, 2, 3, 2], 2) -> [1, 3]
  *   arrayRemove(['a', NULL, 'b', NULL], NULL) -> ['a', 'b']
  */

class FunctionArrayRemove : public IFunction
{
public:
    static constexpr auto name = "arrayRemove";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayRemove>(context_); }
    explicit FunctionArrayRemove(ContextPtr context_) : context(context_) {}

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override;

private:
    ContextPtr context;
};

}
