#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Following class implement IExecutableFunction via IFunction.

class FunctionToExecutableFunctionAdaptor final : public IExecutableFunction
{
public:
<<<<<<< HEAD
    explicit FunctionToExecutableFunctionAdaptor(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}
=======
    explicit ExecutableFunctionAdaptor(ExecutableFunctionImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const final;

    void createLowCardinalityResultCache(size_t cache_size) override;

private:
    ExecutableFunctionImplPtr impl;

    /// Cache is created by function createLowCardinalityResultCache()
    ExecutableFunctionLowCardinalityResultCachePtr low_cardinality_result_cache;

    ColumnPtr defaultImplementationForConstantArguments(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    ColumnPtr defaultImplementationForNulls(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    ColumnPtr executeWithoutLowCardinalityColumns(
            const ColumnsWithTypeAndName & args, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;
};

class FunctionBaseAdaptor final : public IFunctionBase
{
public:
    explicit FunctionBaseAdaptor(FunctionBaseImplPtr impl_) : impl(std::move(impl_)) {}

    String getName() const final { return impl->getName(); }

    const DataTypes & getArgumentTypes() const final { return impl->getArgumentTypes(); }
    const DataTypePtr & getResultType() const final { return impl->getResultType(); }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const final
    {
        return std::make_shared<ExecutableFunctionAdaptor>(impl->prepare(arguments));
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const final { return impl->isCompilable(); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, Values values) const override
    {
        return impl->compile(builder, std::move(values));
    }

#endif

    bool isStateful() const final { return impl->isStateful(); }
    bool isSuitableForConstantFolding() const final { return impl->isSuitableForConstantFolding(); }

    ColumnPtr getResultIfAlwaysReturnsConstantAndHasArguments(const ColumnsWithTypeAndName & arguments) const final
    {
        return impl->getResultIfAlwaysReturnsConstantAndHasArguments(arguments);
    }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const final { return impl->isInjective(sample_columns); }
    bool isDeterministic() const final { return impl->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const final { return impl->isDeterministicInScopeOfQuery(); }
    bool isShortCircuit() const final { return impl->isShortCircuit(); }

    void executeShortCircuitArguments(ColumnsWithTypeAndName & arguments) const override
    {
        impl->executeShortCircuitArguments(arguments);
    }

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

    bool isInjective(const ColumnsWithTypeAndName & columns) const final { return impl->isInjective(columns); }

    bool isStateful() const final { return impl->isStateful(); }

    bool isVariadic() const final { return impl->isVariadic(); }

    bool isShortCircuit() const final { return impl->isShortCircuit(); }

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

    using DefaultReturnTypeGetter = std::function<DataTypePtr(const ColumnsWithTypeAndName &)>;
    static DataTypePtr getReturnTypeDefaultImplementationForNulls(const ColumnsWithTypeAndName & arguments, const DefaultReturnTypeGetter & getter);
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
>>>>>>> Fix tests

    String getName() const override { return function->getName(); }

protected:

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final
    {
        return function->executeImpl(arguments, result_type, input_rows_count);
    }

    ColumnPtr executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final
    {
        return function->executeImplDryRun(arguments, result_type, input_rows_count);
    }

    bool useDefaultImplementationForNulls() const final { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForConstants() const final { return function->useDefaultImplementationForConstants(); }
    bool useDefaultImplementationForLowCardinalityColumns() const final { return function->useDefaultImplementationForLowCardinalityColumns(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return function->getArgumentsThatAreAlwaysConstant(); }
    bool canBeExecutedOnDefaultArguments() const override { return function->canBeExecutedOnDefaultArguments(); }

private:
    std::shared_ptr<IFunction> function;
};

/// Following class implement IFunctionBase via IFunction.

class FunctionToFunctionBaseAdaptor final : public IFunctionBase
{
public:
    FunctionToFunctionBaseAdaptor(std::shared_ptr<IFunction> function_, DataTypes arguments_, DataTypePtr result_type_)
            : function(std::move(function_)), arguments(std::move(arguments_)), result_type(std::move(result_type_)) {}

    String getName() const override { return function->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getResultType() const override { return result_type; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override { return function->isCompilable(getArgumentTypes()); }

    llvm::Value * compile(llvm::IRBuilderBase & builder, Values values) const override
    {
        return function->compile(builder, getArgumentTypes(), std::move(values));
    }

#endif

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<FunctionToExecutableFunctionAdaptor>(function);
    }

    bool isSuitableForConstantFolding() const override { return function->isSuitableForConstantFolding(); }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments_, const DataTypePtr & result_type_) const override
    {
        return function->getConstantResultForNonConstArguments(arguments_, result_type_);
    }

    bool isStateful() const override { return function->isStateful(); }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const override { return function->isInjective(sample_columns); }

    bool isDeterministic() const override { return function->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }

    bool isShortCircuit() const override { return function->isShortCircuit(); }

<<<<<<< HEAD
    bool isSuitableForShortCircuitArgumentsExecution() const override { return function->isSuitableForShortCircuitArgumentsExecution(); }

=======
>>>>>>> Fix tests
    void executeShortCircuitArguments(ColumnsWithTypeAndName & args) const override
    {
        function->executeShortCircuitArguments(args);
    }

    bool hasInformationAboutMonotonicity() const override { return function->hasInformationAboutMonotonicity(); }

    Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return function->getMonotonicityForRange(type, left, right);
    }
private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr result_type;
};


/// Following class implement IFunctionOverloadResolver via IFunction.

class FunctionToOverloadResolverAdaptor : public IFunctionOverloadResolver
{
public:
    explicit FunctionToOverloadResolverAdaptor(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

    bool isDeterministic() const override { return function->isDeterministic(); }
    bool isDeterministicInScopeOfQuery() const override { return function->isDeterministicInScopeOfQuery(); }
    bool isInjective(const ColumnsWithTypeAndName & columns) const override { return function->isInjective(columns); }

    String getName() const override { return function->getName(); }
    bool isStateful() const override { return function->isStateful(); }
    bool isVariadic() const override { return function->isVariadic(); }
    bool isShortCircuit() const override { return function->isShortCircuit(); }
<<<<<<< HEAD
    bool isSuitableForShortCircuitArgumentsExecution() const override { return function->isSuitableForShortCircuitArgumentsExecution(); }
=======

    void executeShortCircuitArguments(ColumnsWithTypeAndName & arguments) const override
    {
        function->executeShortCircuitArguments(arguments);
    }

>>>>>>> Fix tests
    size_t getNumberOfArguments() const override { return function->getNumberOfArguments(); }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return function->getArgumentsThatAreAlwaysConstant(); }
    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t number_of_arguments) const override
    {
        return function->getArgumentsThatDontImplyNullableReturnType(number_of_arguments);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return function->getReturnTypeImpl(arguments); }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override { return function->getReturnTypeImpl(arguments); }

    bool useDefaultImplementationForNulls() const override { return function->useDefaultImplementationForNulls(); }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return function->useDefaultImplementationForLowCardinalityColumns(); }
    bool canBeExecutedOnLowCardinalityDictionary() const override { return function->canBeExecutedOnLowCardinalityDictionary(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            data_types[i] = arguments[i].type;

        return std::make_unique<FunctionToFunctionBaseAdaptor>(function, data_types, result_type);
    }

    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override { function->getLambdaArgumentTypes(arguments); }

private:
    std::shared_ptr<IFunction> function;
};


}
