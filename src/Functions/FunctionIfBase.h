#pragma once

#include <Functions/IFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/VectorWithMemoryTracking.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/ADT/StringExtras.h>
#    include <llvm/IR/IRBuilder.h>
#    include <Core/DecimalFunctions.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/Native.h>
#    include <Interpreters/JIT/JITError.h>
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

            if (scaleLiftIsNotCompilable(*type_removed_nullable, *result_nested))
                return false;
        }

        return true;
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);

        /// The interpreter casts every row of each branch it does not skip to the result type before it picks the values
        /// (`castColumn` in `FunctionMultiIf::executeImpl` and `FunctionIf::executeImpl`), so a `Decimal` lift that
        /// overflows fails on any row, whether the row takes the branch or not. Check the lifts before the branches, in
        /// their order. Like the interpreter, skip the branches of constant false conditions, and stop after a constant true one.
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            const bool is_else = i + 1 == arguments.size();
            bool always_taken = is_else;
            if (!is_else)
            {
                if (auto * constant = llvm::dyn_cast<llvm::ConstantInt>(nativeBoolCast(b, arguments[i])))
                {
                    if (constant->isZero())
                        continue;
                    always_taken = true;
                }
            }
            reportDecimalLiftOverflow(b, is_else ? arguments[i] : arguments[i + 1], result_type);
            if (always_taken)
                break;
        }

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
    /// Compiled code cannot clamp, so a `DateTime64` or `Time64` branch whose lift to the result scale can leave 32- or
    /// 64-bit storage, where the interpreted cast clamps to the range of the result, is not compilable. Neither is a
    /// branch with wider storage than the result, which the interpreted cast checks with another message.
    /// A `Decimal` lift that overflows raises `DECIMAL_OVERFLOW`, see `reportDecimalLiftOverflow`.
    static bool scaleLiftIsNotCompilable(const IDataType & branch, const IDataType & result)
    {
        if (isDecimal(branch) && isDecimal(result))
            return branch.getSizeOfValueInMemory() > result.getSizeOfValueInMemory();
        const bool same_family = (isDateTime64(branch) || isTime64(branch)) && (isDateTime64(result) || isTime64(result));
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

    /// Reports the rows where lifting the `Decimal` `branch` to the scale of the `Decimal` result overflows the storage of
    /// the result, with the error of `convertDecimalsBatch`. Nothing is reported if the lift cannot overflow.
    static void reportDecimalLiftOverflow(llvm::IRBuilder<> & b, const ValueWithType & branch, const DataTypePtr & result_type)
    {
        const auto branch_type = removeNullable(branch.type);
        const auto result_nested = removeNullable(result_type);
        if (!isDecimal(*branch_type) || !isDecimal(*result_nested))
            return;
        const UInt32 branch_scale = getDecimalScale(*branch_type);
        const UInt32 result_scale = getDecimalScale(*result_nested);
        if (result_scale <= branch_scale)
            return;

        const unsigned branch_bits = static_cast<unsigned>(branch_type->getSizeOfValueInMemory() * 8);
        const unsigned result_bits = static_cast<unsigned>(result_nested->getSizeOfValueInMemory() * 8);
        llvm::APInt multiplier(result_bits, 1);
        for (UInt32 i = branch_scale; i < result_scale; ++i)
            multiplier *= 10;
        /// The values whose product with the multiplier fits, as `DecimalScaleBounds`.
        const llvm::APInt max_value = llvm::APInt::getSignedMaxValue(result_bits).sdiv(multiplier);
        const llvm::APInt min_value = llvm::APInt::getSignedMinValue(result_bits).sdiv(multiplier);
        if (llvm::APInt::getSignedMaxValue(branch_bits).sext(result_bits).sle(max_value)
            && llvm::APInt::getSignedMinValue(branch_bits).sext(result_bits).sge(min_value))
            return;

        auto * value = branch.type->isNullable() ? b.CreateExtractValue(branch.value, {0}) : branch.value;
        auto * widened = b.CreateSExtOrTrunc(value, b.getIntNTy(result_bits));
        auto * fails = b.CreateOr(
            b.CreateICmpSLT(widened, llvm::ConstantInt::get(b.getContext(), min_value)),
            b.CreateICmpSGT(widened, llvm::ConstantInt::get(b.getContext(), max_value)));

        JITErrorValue error;
        addJITError(b, error, fails, JITErrorCode::DecimalConvertOverflow);
        error.operand = value;
        reportJITError(b, error, {DataTypeDecimal<Decimal32>::family_name, llvm::toString(multiplier, 10, /*Signed=*/true)});
    }
#endif
};

}
