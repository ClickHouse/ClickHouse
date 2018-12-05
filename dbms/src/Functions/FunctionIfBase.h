#pragma once

#include <Common/config.h>
#include <Functions/IFunction.h>
#include <DataTypes/Native.h>


namespace DB
{

template <bool null_is_false>
class FunctionIfBase : public IFunction
{
#if USE_EMBEDDED_COMPILER
public:
    bool isCompilableImpl(const DataTypes & types) const override
    {
        for (const auto & type : types)
            if (!isCompilableType(removeNullable(type)))
                return false;
        return true;
    }

    llvm::Value * compileImpl(llvm::IRBuilderBase & builder, const DataTypes & types, ValuePlaceholders values) const override
    {
        auto & b = static_cast<llvm::IRBuilder<> &>(builder);
        auto type = getReturnTypeImpl(types);
        llvm::Value * null = nullptr;
        if (!null_is_false && type->isNullable())
            null = b.CreateInsertValue(llvm::Constant::getNullValue(toNativeType(b, type)), b.getTrue(), {1});
        auto * head = b.GetInsertBlock();
        auto * join = llvm::BasicBlock::Create(head->getContext(), "", head->getParent());
        std::vector<std::pair<llvm::BasicBlock *, llvm::Value *>> returns;
        for (size_t i = 0; i + 1 < types.size(); i += 2)
        {
            auto * then = llvm::BasicBlock::Create(head->getContext(), "", head->getParent());
            auto * next = llvm::BasicBlock::Create(head->getContext(), "", head->getParent());
            auto * cond = values[i]();
            if (!null_is_false && types[i]->isNullable())
            {
                auto * nonnull = llvm::BasicBlock::Create(head->getContext(), "", head->getParent());
                returns.emplace_back(b.GetInsertBlock(), null);
                b.CreateCondBr(b.CreateExtractValue(cond, {1}), join, nonnull);
                b.SetInsertPoint(nonnull);
                b.CreateCondBr(nativeBoolCast(b, removeNullable(types[i]), b.CreateExtractValue(cond, {0})), then, next);
            }
            else
            {
                b.CreateCondBr(nativeBoolCast(b, types[i], cond), then, next);
            }
            b.SetInsertPoint(then);
            auto * value = nativeCast(b, types[i + 1], values[i + 1](), type);
            returns.emplace_back(b.GetInsertBlock(), value);
            b.CreateBr(join);
            b.SetInsertPoint(next);
        }
        auto * value = nativeCast(b, types.back(), values.back()(), type);
        returns.emplace_back(b.GetInsertBlock(), value);
        b.CreateBr(join);
        b.SetInsertPoint(join);
        auto * phi = b.CreatePHI(toNativeType(b, type), returns.size());
        for (const auto & r : returns)
            phi->addIncoming(r.second, r.first);
        return phi;
    }
#endif
};

}
