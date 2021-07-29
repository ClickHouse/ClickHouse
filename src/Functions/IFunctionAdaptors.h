#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Following class implement IExecutableFunction via IFunction.

class FunctionToExecutableFunctionAdaptor final : public IExecutableFunction
{
public:
    explicit FunctionToExecutableFunctionAdaptor(std::shared_ptr<IFunction> function_) : function(std::move(function_)) {}

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
