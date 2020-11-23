#pragma once
#include <Functions/IFunctionImpl.h>

namespace DB
{

/// Adaptors are implement user interfaces from IFunction.h via developer interfaces from IFunctionImpl.h
/// Typically, you don't need to change this classes.

class ExecutableFunctionAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionAdaptor(ExecutableFunctionImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count, bool dry_run) final;

    void createLowCardinalityResultCache(size_t cache_size) override;

private:
    ExecutableFunctionImplPtr impl;

    /// Cache is created by function createLowCardinalityResultCache()
    ExecutableFunctionLowCardinalityResultCachePtr low_cardinality_result_cache;

    bool defaultImplementationForConstantArguments(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);

    bool defaultImplementationForNulls(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);

    void executeWithoutLowCardinalityColumns(
            Block & block, const ColumnNumbers & args, size_t result, size_t input_rows_count, bool dry_run);
};

class FunctionBaseAdaptor final : public IFunctionBase
{
public:
    explicit FunctionBaseAdaptor(FunctionBaseImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    const DataTypes & getArgumentTypes() const final { return impl->getArgumentTypes(); }
    const DataTypePtr & getReturnType() const final { return impl->getReturnType(); }

    ExecutableFunctionPtr prepare(const Block & sample_block, const ColumnNumbers & arguments, size_t result) const final
    {
        return std::make_shared<ExecutableFunctionAdaptor>(impl->prepare(sample_block, arguments, result));
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const final { return impl->isCompilable(); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override
    {
        return impl->compile(builder, std::move(values));
    }

#endif

    bool isStateful() const final { return impl->isStateful(); }
    bool isSuitableForConstantFolding() const final { return impl->isSuitableForConstantFolding(); }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments) const final
    {
        return impl->getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments);
    }

    bool isInjective(const Block & sample_block) const final { return impl->isInjective(sample_block); }
    bool isDeterministic() const final { return impl->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const final { return impl->isDeterministicInScopeOfQuery(); }
    bool hasInformationAboutMonotonicity() const final { return impl->hasInformationAboutMonotonicity(); }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const final
    {
        return impl->getMonotonicityForRange(type, left, right);
    }

    const IFunctionBaseImpl * getImpl() const { return impl.get(); }

private:
    FunctionBaseImplPtr impl;
};


class FunctionOverloadResolverAdaptor final : public IFunctionOverloadResolver
{
public:
    explicit FunctionOverloadResolverAdaptor(FunctionOverloadResolverImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    bool isDeterministic() const final { return impl->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const final { return impl->isDeterministicInScopeOfQuery(); }

    bool isInjective(const Block & block) const final { return impl->isInjective(block); }

    bool isStateful() const final { return impl->isStateful(); }

    bool isVariadic() const final { return impl->isVariadic(); }

    size_t getNumberOfArguments() const final { return impl->getNumberOfArguments(); }

    void checkNumberOfArguments(size_t number_of_arguments) const final;

    FunctionBaseImplPtr buildImpl(const ColumnsWithTypeAndName & arguments) const
    {
        return impl->build(arguments, getReturnType(arguments));
    }

    FunctionBasePtr build(const ColumnsWithTypeAndName & arguments) const final
    {
        return std::make_shared<FunctionBaseAdaptor>(buildImpl(arguments));
    }

    void getLambdaArgumentTypes(DataTypes & arguments) const final
    {
        checkNumberOfArguments(arguments.size());
        impl->getLambdaArgumentTypes(arguments);
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return impl->getArgumentsThatAreAlwaysConstant(); }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const final
    {
        return impl->getArgumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

private:
    FunctionOverloadResolverImplPtr impl;

    DataTypePtr getReturnTypeWithoutLowCardinality(const ColumnsWithTypeAndName & arguments) const;
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const;
};


/// Following classes are implement IExecutableFunctionImpl, IFunctionBaseImpl and IFunctionOverloadResolverImpl via IFunction.

class DefaultExecutable final : public IExecutableFunctionImpl
{
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

    String getName() const override { return function->getName(); }

protected:
    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) final
    {
        return function->executeImpl(block, arguments, result, input_rows_count);
    }
    void executeDryRun(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) final
    {
        return function->executeImplDryRun(block, arguments, result, input_rows_count);
    }
    bool useDefaultImplementationForNulls() const final { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForConstants() const final { return function->useDefaultImplementationForConstants(); }
    bool useDefaultImplementationForLowCardinalityColumns() const final { return function->useDefaultImplementationForLowCardinalityColumns(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return function->getArgumentsThatAreAlwaysConstant(); }
    bool canBeExecutedOnDefaultArguments() const override { return function->canBeExecutedOnDefaultArguments(); }

private:
    std::shared_ptr<IFunction> function;
};

class DefaultFunction final : public IFunctionBaseImpl
{
public:
    DefaultFunction(std::shared_ptr<IFunction> function_, DataTypes arguments_, DataTypePtr return_type_)
            : function(std::move(function_)), arguments(std::move(arguments_)), return_type(std::move(return_type_)) {}

    String getName() const override { return function->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getReturnType() const override { return return_type; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override { return function->isCompilable(arguments); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, ValuePlaceholders values) const override { return function->compile(builder, arguments, std::move(values)); }

#endif

    ExecutableFunctionImplPtr prepare(const Block & /*sample_block*/, const ColumnNumbers & /*arguments*/, size_t /*result*/) const override
    {
        return std::make_unique<DefaultExecutable>(function);
    }

    bool isSuitableForConstantFolding() const override { return function->isSuitableForConstantFolding(); }
    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const Block & block, const ColumnNumbers & arguments_) const override
    {
        return function->getResultIfAlwaysReturnsConstantAndHasArguments(block, arguments_);
    }

    bool isStateful() const override { return function->isStateful(); }

    bool isInjective(const Block & sample_block) const override { return function->isInjective(sample_block); }

    bool isDeterministic() const override { return function->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return function->hasInformationAboutMonotonicity(); }

    using Monotonicity = IFunctionBase::Monotonicity;
    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return function->getMonotonicityForRange(type, left, right);
    }
private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    explicit DefaultOverloadResolver(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

    bool isDeterministic() const override { return function->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }
    bool isInjective(const Block &block) const override { return function->isInjective(block); }

    String getName() const override { return function->getName(); }
    bool isStateful() const override { return function->isStateful(); }
    bool isVariadic() const override { return function->isVariadic(); }
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return function->getArgumentsThatAreAlwaysConstant(); }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        return function->getArgumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

    DataTypePtr getReturnType(const DataTypes & arguments) const override { return function->getReturnTypeImpl(arguments); }
    DataTypePtr getReturnType(const ColumnsWithTypeAndName & arguments) const override { return function->getReturnTypeImpl(arguments); }

    bool useDefaultImplementationForNulls() const override { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return function->useDefaultImplementationForLowCardinalityColumns(); }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return function->canBeExecutedOnLowCardinalityDictionary(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;
        return std::make_unique<DefaultFunction>(function, data_types, return_type);
    }

    void getLambdaArgumentTypes(DataTypes & arguments) const override { function->getLambdaArgumentTypes(arguments); }

private:
    std::shared_ptr<IFunction> function;
};


}
