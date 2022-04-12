#pragma once

#include <Functions/IFunction.h>
#include <DataTypes/Native.h>

#include <Common/config.h>

namespace DB
{

class FunctionIfBase : public IFunction
{
#if USE_EMBEDDED_COMPILER
public:
    bool isCompilableImpl(const DataTypes & types) const override
    {
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

            if (!isCompilableType(type_removed_nullable))
                return false;
        }
        return true;
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, Values values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        auto return_type = getReturnTypeImpl(types);

        auto * head = b.GetInsertBlock();
        auto * join = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());

        std::vector<std::pair<llvm::BasicBlock *, llvm::Value *>> returns;
        for (size_t i = 0; i + 1 < types.size(); i += 2)
        {
            auto * then = llvm::BasicBlock::Create(head->getContext(), "then_" + std::to_string(i), head->getParent());
            auto * next = llvm::BasicBlock::Create(head->getContext(), "next_" + std::to_string(i), head->getParent());
            auto * cond = values[i];

            b.CreateCondBr(nativeBoolCast(b, types[i], cond), then, next);
            b.SetInsertPoint(then);

            auto * value = nativeCast(b, types[i + 1], values[i + 1], return_type);
            returns.emplace_back(b.GetInsertBlock(), value);
            b.CreateBr(join);
            b.SetInsertPoint(next);
        }

        auto * else_value = nativeCast(b, types.back(), values.back(), return_type);
        returns.emplace_back(b.GetInsertBlock(), else_value);
        b.CreateBr(join);

        b.SetInsertPoint(join);

        auto * phi = b.CreatePHI(toNativeType(b, return_type), returns.size());
        for (const auto & [block, value] : returns)
            phi->addIncoming(value, block);

        return phi;
    }
#endif
};

}
