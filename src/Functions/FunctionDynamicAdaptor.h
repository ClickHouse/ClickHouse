#pragma once

#include <Functions/IFunction.h>

namespace DB
{

/// Special adapter classes that implement functions execution with Dynamic arguments.

class ExecutableFunctionDynamicAdaptor final : public IExecutableFunction
{
public:
    explicit ExecutableFunctionDynamicAdaptor(
        std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_, size_t dynamic_argument_index_)
        : function_overload_resolver(std::move(function_overload_resolver_)), dynamic_argument_index(dynamic_argument_index_)
    {
    }

    String getName() const override { return function_overload_resolver->getName(); }

protected:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final;
    ColumnPtr executeDryRunImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const final;

    /// Disable all default implementations, they will be used later.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForSparseColumns() const override { return false; }
    bool canBeExecutedOnDefaultArguments() const override { return false; }

private:
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const;

    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Dynamic column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    size_t dynamic_argument_index;
};

class FunctionBaseDynamicAdaptor final : public IFunctionBase
{
public:
    FunctionBaseDynamicAdaptor(std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver_, DataTypes arguments_);

    String getName() const override { return function_overload_resolver->getName(); }

    const DataTypes & getArgumentTypes() const override { return arguments; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_unique<ExecutableFunctionDynamicAdaptor>(function_overload_resolver, dynamic_argument_index);
    }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isStateful() const override { return function_overload_resolver->isStateful(); }

    bool isDeterministic() const override { return function_overload_resolver->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return function_overload_resolver->isDeterministicInScopeOfQuery(); }

    bool isServerConstant() const override { return function_overload_resolver->isServerConstant(); }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t number_of_arguments) const override { return function_overload_resolver->isShortCircuit(settings, number_of_arguments); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

private:
    /// We remember the original IFunctionOverloadResolver to be able to build function for types inside Dynamic column.
    std::shared_ptr<const IFunctionOverloadResolver> function_overload_resolver;
    DataTypes arguments;
    DataTypePtr return_type;
    size_t dynamic_argument_index;
};


}
