#pragma once

#include "config.h"

#if USE_EMBEDDED_COMPILER

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Native.h>
#include <Functions/IFunction.h>

#include <llvm/IR/IRBuilder.h>


namespace DB
{

/** The compiled form of `coalesce` and `ifNull`: the first argument that is not NULL, converted to the result type.
  * The interpreted functions run `multiIf(isNotNull(x0), assumeNotNull(x0), ..., xn)`, which converts each value to the
  * result type with `castColumn`; compiled code does the same conversion with `nativeCast`, which is exact for numbers.
  * `arguments` are the arguments the function keeps: no NULL literal, and none after the first that cannot be NULL.
  */
inline bool canCompileFirstNonNull(const DataTypes & arguments, const DataTypePtr & result_type)
{
    if (arguments.empty() || !canBeNativeType(*result_type))
        return false;

    /// Numbers (not `Decimal`, whose scale lifts are checked in `FunctionIfBase`), or values of the result type itself.
    const auto result_nested = removeNullable(result_type);
    const bool result_is_number = isInteger(result_nested) || isNativeFloat(result_nested);
    for (const auto & argument : arguments)
    {
        const auto nested = removeNullable(argument);
        if (!canBeNativeType(*nested))
            return false;
        const bool is_number = isInteger(nested) || isNativeFloat(nested);
        if (!(result_is_number && is_number) && !nested->equals(*result_nested))
            return false;
    }
    return true;
}

inline llvm::Value * compileFirstNonNull(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type)
{
    auto & b = static_cast<llvm::IRBuilder<> &>(builder);
    const auto result_nested = removeNullable(result_type);

    /// From the last argument to the first: an argument that is not NULL replaces the value of those after it.
    llvm::Value * value = nullptr;
    llvm::Value * is_null = nullptr;
    for (size_t i = arguments.size(); i > 0; --i)
    {
        const auto & argument = arguments[i - 1];
        llvm::Value * nested = argument.value;
        llvm::Value * argument_is_null = b.getFalse();
        if (argument.type->isNullable())
        {
            nested = b.CreateExtractValue(argument.value, {0});
            argument_is_null = b.CreateExtractValue(argument.value, {1});
        }
        auto * converted = nativeCast(b, removeNullable(argument.type), nested, result_nested);

        if (!value)
        {
            value = converted;
            is_null = argument_is_null;
            continue;
        }
        value = b.CreateSelect(argument_is_null, value, converted);
        is_null = b.CreateAnd(argument_is_null, is_null);
    }

    if (!result_type->isNullable())
        return value;
    auto * result = llvm::Constant::getNullValue(toNativeType(b, result_type));
    return b.CreateInsertValue(b.CreateInsertValue(result, value, {0}), is_null, {1});
}

}

#endif
