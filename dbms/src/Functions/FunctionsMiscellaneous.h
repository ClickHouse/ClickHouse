#pragma once

#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/ColumnFunction.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

/** Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
  */
class FunctionReplicate : public IFunction
{
public:
    static constexpr auto name = "replicate";
    static FunctionPtr create(const Context & context);

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override;
};


/// Executes expression. Uses for lambda functions implementation. Can't be created from factory.
class FunctionExpression : public IFunctionBase, public IPreparedFunction,
                           public std::enable_shared_from_this<FunctionExpression>
{
public:
    FunctionExpression(const ExpressionActionsPtr & expression_actions,
                       const DataTypes & argument_types, const Names & argument_names,
                       const DataTypePtr & return_type, const std::string & return_name)
            : expression_actions(expression_actions), argument_types(argument_types),
              argument_names(argument_names), return_type(return_type), return_name(return_name)
    {
    }

    String getName() const override { return "FunctionExpression"; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block &) const override
    {
        return std::const_pointer_cast<FunctionExpression>(shared_from_this());
    }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) override
    {
        Block expr_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = block.getByPosition(arguments[i]);
            /// Replace column name with value from argument_names.
            expr_block.insert({argument.column, argument.type, argument_names[i]});
        }

        expression_actions->execute(expr_block);

        block.getByPosition(result).column = expr_block.getByName(return_name).column;
    }

private:
    ExpressionActionsPtr expression_actions;
    DataTypes argument_types;
    Names argument_names;
    DataTypePtr return_type;
    std::string return_name;
};

/// Captures columns which are used by lambda function but not in argument list.
/// Returns ColumnFunction with captured columns.
/// For lambda(x, x + y) x is in lambda_arguments, y is in captured arguments, expression_actions is 'x + y'.
///  execute(y) returns ColumnFunction(FunctionExpression(x + y), y) with type Function(x) -> function_return_type.
class FunctionCapture : public IFunctionBase, public IPreparedFunction, public FunctionBuilderImpl,
                        public std::enable_shared_from_this<FunctionCapture>
{
public:
    FunctionCapture(const ExpressionActionsPtr & expression_actions, const Names & captured,
                    const NamesAndTypesList & lambda_arguments,
                    const DataTypePtr & function_return_type, const std::string & expression_return_name)
            : expression_actions(expression_actions), captured_names(captured), lambda_arguments(lambda_arguments)
            , function_return_type(function_return_type), expression_return_name(expression_return_name)
    {
        const auto & all_arguments = expression_actions->getRequiredColumnsWithTypes();

        std::unordered_map<std::string, DataTypePtr> arguments_map;
        for (const auto & arg : all_arguments)
            arguments_map[arg.name] = arg.type;

        auto collect = [&arguments_map](const Names & names)
        {
            DataTypes types;
            types.reserve(names.size());
            for (const auto & captured_name : names)
            {
                auto it = arguments_map.find(captured_name);
                if (it == arguments_map.end())
                    throw Exception("Lambda captured argument " + captured_name + " not found in required columns.",
                                    ErrorCodes::LOGICAL_ERROR);

                types.push_back(it->second);
                arguments_map.erase(it);
            }

            return types;
        };

        captured_types = collect(captured_names);

        DataTypes argument_types;
        argument_types.reserve(lambda_arguments.size());
        for (const auto & lambda_argument : lambda_arguments)
            argument_types.push_back(lambda_argument.type);

        return_type = std::make_shared<DataTypeFunction>(argument_types, function_return_type);

        name = "Capture[" + toString(captured_types) + "](" + toString(argument_types) +  ") -> "
               + function_return_type->getName();
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return captured_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    PreparedFunctionPtr prepare(const Block &) const override
    {
        return std::const_pointer_cast<FunctionCapture>(shared_from_this());
    }

    void execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        ColumnsWithTypeAndName columns;
        columns.reserve(arguments.size());

        Names names;
        DataTypes types;

        names.reserve(captured_names.size() + lambda_arguments.size());
        names.insert(names.end(), captured_names.begin(), captured_names.end());

        types.reserve(captured_types.size() + lambda_arguments.size());
        types.insert(types.end(), captured_types.begin(), captured_types.end());

        for (const auto & lambda_argument : lambda_arguments)
        {
            names.push_back(lambda_argument.name);
            types.push_back(lambda_argument.type);
        }

        for (const auto & argument : arguments)
            columns.push_back(block.getByPosition(argument));

        auto function = std::make_shared<FunctionExpression>(expression_actions, types, names,
                                                             function_return_type, expression_return_name);
        block.getByPosition(result).column = ColumnFunction::create(input_rows_count, std::move(function), columns);
    }

    size_t getNumberOfArguments() const override { return captured_types.size(); }

protected:
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return return_type; }
    bool useDefaultImplementationForNulls() const override { return false; }
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::const_pointer_cast<FunctionCapture>(shared_from_this());
    }

private:
    std::string toString(const DataTypes & data_types) const
    {
        std::string result;
        {
            WriteBufferFromString buffer(result);
            bool first = true;
            for (const auto & type : data_types)
            {
                if (!first)
                    buffer << ", ";

                first = false;
                buffer << type->getName();
            }
        }

        return result;
    }

    ExpressionActionsPtr expression_actions;
    DataTypes captured_types;
    Names captured_names;
    NamesAndTypesList lambda_arguments;
    DataTypePtr function_return_type;
    DataTypePtr return_type;
    std::string expression_return_name;
    std::string name;
};

}
