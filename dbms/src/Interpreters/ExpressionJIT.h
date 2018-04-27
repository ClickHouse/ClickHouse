#pragma once

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER

#include <Functions/IFunction.h>

#include <Interpreters/ExpressionActions.h>

namespace DB
{

class LLVMContext
{
    struct Data;
    std::shared_ptr<Data> shared;

public:
    LLVMContext();

    void finalize();

    bool isCompilable(const IFunctionBase& function) const;

    Data * operator->() const {
        return shared.get();
    }
};

class LLVMPreparedFunction : public PreparedFunctionImpl
{
    std::shared_ptr<const IFunctionBase> parent;
    LLVMContext context;
    const void * function;

public:
    LLVMPreparedFunction(LLVMContext context, std::shared_ptr<const IFunctionBase> parent);

    String getName() const override { return parent->getName(); }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override;
};

class LLVMFunction : public std::enable_shared_from_this<LLVMFunction>, public IFunctionBase
{
    /// all actions must have type APPLY_FUNCTION
    ExpressionActions::Actions actions;
    Names arg_names;
    DataTypes arg_types;
    LLVMContext context;

public:
    LLVMFunction(ExpressionActions::Actions actions, LLVMContext context, const Block & sample_block);

    String getName() const override { return actions.back().result_name; }

    const Names & getArgumentNames() const { return arg_names; }

    const DataTypes & getArgumentTypes() const override { return arg_types; }

    const DataTypePtr & getReturnType() const override { return actions.back().function->getReturnType(); }

    PreparedFunctionPtr prepare(const Block &) const override { return std::make_shared<LLVMPreparedFunction>(context, shared_from_this()); }

    bool isDeterministic() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministic())
                return false;
        return true;
    }

    bool isDeterministicInScopeOfQuery() override
    {
        for (const auto & action : actions)
            if (!action.function->isDeterministicInScopeOfQuery())
                return false;
        return true;
    }

    bool isSuitableForConstantFolding() const override
    {
        for (const auto & action : actions)
            if (!action.function->isSuitableForConstantFolding())
                return false;
        return true;
    }

    bool isInjective(const Block & sample_block) override
    {
        for (const auto & action : actions)
            if (!action.function->isInjective(sample_block))
                return false;
        return true;
    }

    bool hasInformationAboutMonotonicity() const override
    {
        for (const auto & action : actions)
            if (!action.function->hasInformationAboutMonotonicity())
                return false;
        return true;
    }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override;
};

}

#endif
