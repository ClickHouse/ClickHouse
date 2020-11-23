#pragma once

#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeFunction.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/ColumnFunction.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

class ExecutableFunctionExpression : public IExecutableFunctionImpl
{
public:
    struct Signature
    {
        Names argument_names;
        String return_name;
    };

    using SignaturePtr = std::shared_ptr<Signature>;

    ExecutableFunctionExpression(ExpressionActionsPtr expression_actions_, SignaturePtr signature_)
        : expression_actions(std::move(expression_actions_))
        , signature(std::move(signature_))
    {}

    String getName() const override { return "FunctionExpression"; }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        DB::Block expr_columns;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            /// Replace column name with value from argument_names.
            expr_columns.insert({argument.column, argument.type, signature->argument_names[i]});
        }

        expression_actions->execute(expr_columns);

         return expr_columns.getByName(signature->return_name).column;
    }

bool useDefaultImplementationForNulls() const override { return false; }

private:
    ExpressionActionsPtr expression_actions;
    SignaturePtr signature;
};

/// Executes expression. Uses for lambda functions implementation. Can't be created from factory.
class FunctionExpression : public IFunctionBaseImpl
{
public:
    using Signature = ExecutableFunctionExpression::Signature;
    using SignaturePtr = ExecutableFunctionExpression::SignaturePtr;

    FunctionExpression(ExpressionActionsPtr expression_actions_,
            DataTypes argument_types_, const Names & argument_names_,
            DataTypePtr return_type_, const std::string & return_name_)
            : expression_actions(std::move(expression_actions_))
            , signature(std::make_shared<Signature>(Signature{argument_names_, return_name_}))
            , argument_types(std::move(argument_types_)), return_type(std::move(return_type_))
    {
    }

    String getName() const override { return "FunctionExpression"; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionExpression>(expression_actions, signature);
    }

private:
    ExpressionActionsPtr expression_actions;
    SignaturePtr signature;
    DataTypes argument_types;
    DataTypePtr return_type;
};

/// Captures columns which are used by lambda function but not in argument list.
/// Returns ColumnFunction with captured columns.
/// For lambda(x, x + y) x is in lambda_arguments, y is in captured arguments, expression_actions is 'x + y'.
///  execute(y) returns ColumnFunction(FunctionExpression(x + y), y) with type Function(x) -> function_return_type.
class ExecutableFunctionCapture : public IExecutableFunctionImpl
{
public:
    struct Capture
    {
        Names captured_names;
        DataTypes captured_types;
        NamesAndTypesList lambda_arguments;
        String return_name;
        DataTypePtr return_type;
    };

    using CapturePtr = std::shared_ptr<Capture>;

    ExecutableFunctionCapture(ExpressionActionsPtr expression_actions_, CapturePtr capture_)
        : expression_actions(std::move(expression_actions_)), capture(std::move(capture_)) {}

    String getName() const override { return "FunctionCapture"; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        Names names;
        DataTypes types;

        names.reserve(capture->captured_names.size() + capture->lambda_arguments.size());
        names.insert(names.end(), capture->captured_names.begin(), capture->captured_names.end());

        types.reserve(capture->captured_types.size() + capture->lambda_arguments.size());
        types.insert(types.end(), capture->captured_types.begin(), capture->captured_types.end());

        for (const auto & lambda_argument : capture->lambda_arguments)
        {
            names.push_back(lambda_argument.name);
            types.push_back(lambda_argument.type);
        }

        auto function = std::make_unique<FunctionExpression>(expression_actions, types, names,
                                                             capture->return_type, capture->return_name);
        auto function_adaptor = std::make_shared<FunctionBaseAdaptor>(std::move(function));
        return ColumnFunction::create(input_rows_count, std::move(function_adaptor), arguments);
    }

private:
    ExpressionActionsPtr expression_actions;
    CapturePtr capture;
};

class FunctionCapture : public IFunctionBaseImpl
{
public:
    using Capture = ExecutableFunctionCapture::Capture;
    using CapturePtr = ExecutableFunctionCapture::CapturePtr;

    FunctionCapture(
        ExpressionActionsPtr expression_actions_,
        CapturePtr capture_,
        DataTypePtr return_type_,
        String name_)
        : expression_actions(std::move(expression_actions_))
        , capture(std::move(capture_))
        , return_type(std::move(return_type_))
        , name(std::move(name_))
    {
    }

    String getName() const override { return name; }

    bool isDeterministic() const override { return true; }
    bool isDeterministicInScopeOfQuery() const override { return true; }

    const DataTypes & getArgumentTypes() const override { return capture->captured_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionImplPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionCapture>(expression_actions, capture);
    }

private:
    ExpressionActionsPtr expression_actions;
    CapturePtr capture;
    DataTypePtr return_type;
    String name;
};

class FunctionCaptureOverloadResolver : public IFunctionOverloadResolverImpl
{
public:
    using Capture = ExecutableFunctionCapture::Capture;
    using CapturePtr = ExecutableFunctionCapture::CapturePtr;

    FunctionCaptureOverloadResolver(
            ExpressionActionsPtr expression_actions_,
            const Names & captured_names_,
            const NamesAndTypesList & lambda_arguments_,
            const DataTypePtr & function_return_type_,
            const String & expression_return_name_)
        : expression_actions(std::move(expression_actions_))
    {
        /// Check that expression does not contain unusual actions that will break columnss structure.
        for (const auto & action : expression_actions->getActions())
            if (action.node->type == ActionsDAG::ActionType::ARRAY_JOIN)
                throw Exception("Expression with arrayJoin or other unusual action cannot be captured", ErrorCodes::BAD_ARGUMENTS);

        std::unordered_map<std::string, DataTypePtr> arguments_map;

        const auto & all_arguments = expression_actions->getRequiredColumnsWithTypes();
        for (const auto & arg : all_arguments)
            arguments_map[arg.name] = arg.type;

        DataTypes captured_types;
        captured_types.reserve(captured_names_.size());

        for (const auto & captured_name : captured_names_)
        {
            auto it = arguments_map.find(captured_name);
            if (it == arguments_map.end())
                throw Exception("Lambda captured argument " + captured_name + " not found in required columns.",
                                ErrorCodes::LOGICAL_ERROR);

            captured_types.push_back(it->second);
            arguments_map.erase(it);
        }

        DataTypes argument_types;
        argument_types.reserve(lambda_arguments_.size());
        for (const auto & lambda_argument : lambda_arguments_)
            argument_types.push_back(lambda_argument.type);

        return_type = std::make_shared<DataTypeFunction>(argument_types, function_return_type_);

        name = "Capture[" + toString(captured_types) + "](" + toString(argument_types) + ") -> "
               + function_return_type_->getName();

        capture = std::make_shared<Capture>(Capture{
                .captured_names = captured_names_,
                .captured_types = std::move(captured_types),
                .lambda_arguments = lambda_arguments_,
                .return_name = expression_return_name_,
                .return_type = function_return_type_,
        });
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    DataTypePtr getReturnType(const ColumnsWithTypeAndName &) const override { return return_type; }
    size_t getNumberOfArguments() const override { return capture->captured_types.size(); }

    FunctionBaseImplPtr build(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionCapture>(expression_actions, capture, return_type, name);
    }

private:
    ExpressionActionsPtr expression_actions;
    CapturePtr capture;
    DataTypePtr return_type;
    String name;

    static String toString(const DataTypes & data_types)
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
};

}
