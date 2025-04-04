#pragma once


#include <Functions/IFunction.h>
#include <Functions/UserDefined/UserDefinedExecutableFunction.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/ShellCommandSource.h>

namespace DB
{

class UserDefinedFunction final : public IFunction
{
public:
    explicit UserDefinedFunction(
        UserDefinedExecutableFunctionPtr executable_function_,
        ContextPtr context_,
        Array parameters_);

    String getName() const override { return executable_function->getConfiguration().name; }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return executable_function->getConfiguration().arguments.size(); }

    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isDeterministic() const override { return executable_function->getConfiguration().is_deterministic; }
    bool isDeterministicInScopeOfQuery() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override;

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    UserDefinedExecutableFunctionPtr executable_function;
    ContextPtr context;
    String command_with_parameters;
    std::vector<String> command_arguments_with_parameters;
};

}
