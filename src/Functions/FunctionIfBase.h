#pragma once

#include <Functions/IFunction.h>
#include <DataTypes/DataTypeNullable.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
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

        for (const auto & type : types)
        {
            auto type_removed_nullable = removeNullable(type);
            WhichDataType which(type_removed_nullable);

            if (which.isDate())
                has_date = true;
            if (which.isDateTime())
                has_datetime = true;

            if (has_date && has_datetime)
                return false;

            if (!canBeNativeType(type_removed_nullable))
                return false;
        }

        return true;
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * head = b.GetInsertBlock();
        auto * join = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());

        std::vector<std::pair<llvm::BasicBlock *, llvm::Value *>> returns;
        for (size_t i = 0; i + 1 < arguments.size(); i += 2)
        {
            auto * then = llvm::BasicBlock::Create(head->getContext(), "then_" + std::to_string(i), head->getParent());
            auto * next = llvm::BasicBlock::Create(head->getContext(), "next_" + std::to_string(i), head->getParent());
            const auto & cond = arguments[i];

            b.CreateCondBr(nativeBoolCast(b, cond), then, next);
            b.SetInsertPoint(then);

            /// Use `nativeCastWithDecimalScale` to correctly lift integer/float branches to a
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
#endif
};

}
