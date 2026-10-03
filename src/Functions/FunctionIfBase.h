#pragma once

#include <Functions/IFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/VectorWithMemoryTracking.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <Core/DecimalFunctions.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{

class FunctionIfBase : public IFunction
{
#if USE_EMBEDDED_COMPILER
public:
    bool isCompilableImpl(const DataTypes & types, const DataTypePtr & result_type) const override
    {
        if (!canBeNativeType(result_type))
            return false;

        /// It's difficult to compare Date and DateTime - cannot use JIT compilation.
        bool has_date = false;
        bool has_datetime = false;

        const auto result_nested = removeNullable(result_type);

        for (const auto & type : types)
        {
            auto type_removed_nullable = removeNullable(type);
            WhichDataType which(type_removed_nullable);

            if (which.isDateOrDate32())
                has_date = true;
            if (which.isDateTimeOrDateTime64())
                has_datetime = true;

            if (has_date && has_datetime)
                return false;

            if (!canBeNativeType(type_removed_nullable))
                return false;

            if (scaleLiftCanOverflow(*type_removed_nullable, *result_nested))
                return false;
        }

        return true;
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * head = b.GetInsertBlock();
        auto * join = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());

        VectorWithMemoryTracking<std::pair<llvm::BasicBlock *, llvm::Value *>> returns;
        for (size_t i = 0; i + 1 < arguments.size(); i += 2)
        {
            auto * then = llvm::BasicBlock::Create(head->getContext(), "then_" + std::to_string(i), head->getParent());
            auto * next = llvm::BasicBlock::Create(head->getContext(), "next_" + std::to_string(i), head->getParent());
            const auto & cond = arguments[i];

            b.CreateCondBr(nativeBoolCast(b, cond), then, next);
            b.SetInsertPoint(then);

            /// Use `nativeCastWithDecimalScale` to correctly lift integer branches to a
            /// `Decimal` `result_type` (and to convert between `Decimal` types of different scales).
            /// Plain `nativeCast` reinterprets the integer bits without applying the `10^scale`
            /// factor, which silently produces wrong values when the analyzer leaves a non-`Decimal`
            /// branch unconverted (e.g. `if(cond, decimal_col, 1)` with `result_type = Decimal(P, S)`).
            auto * value = nativeCastWithDecimalScale(b, arguments[i + 1], result_type);
            returns.emplace_back(b.GetInsertBlock(), value);
            b.CreateBr(join);
            b.SetInsertPoint(next);
        }

        auto * else_value = nativeCastWithDecimalScale(b, arguments.back(), result_type);
        returns.emplace_back(b.GetInsertBlock(), else_value);
        b.CreateBr(join);

        b.SetInsertPoint(join);

        auto * phi = b.CreatePHI(toNativeType(b, result_type), static_cast<unsigned>(returns.size()));
        for (const auto & [block, value] : returns)
            phi->addIncoming(value, block);

        return phi;
    }

private:
    /// Compiled code cannot raise, so a `Decimal`, `DateTime64` or `Time64` branch whose lift to the result scale can leave
    /// 32- or 64-bit storage, where the interpreted cast raises `DECIMAL_OVERFLOW`, is not compilable.
    static bool scaleLiftCanOverflow(const IDataType & branch, const IDataType & result)
    {
        const bool same_family = (isDecimal(branch) && isDecimal(result))
            || ((isDateTime64(branch) || isTime64(branch)) && (isDateTime64(result) || isTime64(result)));
        if (!same_family || result.getSizeOfValueInMemory() > sizeof(Int64))
            return false;
        if (branch.getSizeOfValueInMemory() > result.getSizeOfValueInMemory())
            return true;
        const UInt32 branch_scale = getDecimalScale(branch);
        const UInt32 result_scale = getDecimalScale(result);
        if (result_scale <= branch_scale)
            return false;
        const bool branch_is_32 = branch.getSizeOfValueInMemory() == sizeof(Int32);
        const Int64 lowest = branch_is_32 ? std::numeric_limits<Int32>::lowest() : std::numeric_limits<Int64>::lowest();
        Int64 lifted = 0;
        if (common::mulOverflow(lowest, DecimalUtils::scaleMultiplier<Int64>(result_scale - branch_scale), lifted))
            return true;
        return result.getSizeOfValueInMemory() == sizeof(Int32) && lifted < std::numeric_limits<Int32>::lowest();
    }
#endif
};

}
