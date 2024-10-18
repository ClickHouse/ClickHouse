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

class ExecutableFunctionExpression : public IExecutableFunction
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        if (!expression_actions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No actions were passed to FunctionExpression");

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
    /// It's possible if expression_actions contains function that don't use
    /// default implementation for Nothing.
    /// Example: arrayMap(x -> CAST(x, 'UInt8'), []);
    bool useDefaultImplementationForNothing() const override { return false; }
    /// Example: SELECT arrayMap(x -> (x + (arrayMap(y -> ((x + y) + toLowCardinality(1)), [])[1])), [])
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

private:
    ExpressionActionsPtr expression_actions;
    SignaturePtr signature;
};

/// Executes expression. Uses for lambda functions implementation. Can't be created from factory.
class FunctionExpression : public IFunctionBase
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
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
class ExecutableFunctionCapture : public IExecutableFunction
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

    ExecutableFunctionCapture(std::shared_ptr<ActionsDAG> actions_dag_, CapturePtr capture_)
        : actions_dag(std::move(actions_dag_)), capture(std::move(capture_)) {}

    String getName() const override { return "FunctionCapture"; }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// It's possible if expression_actions contains function that don't use
    /// default implementation for Nothing and one of captured columns can be Nothing
    /// Example: SELECT arrayMap(x -> [x, arrayElement(y, 0)], []), [] as y
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
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

        return ColumnFunction::create(input_rows_count, std::move(function), arguments);
    }

    void buildExpressionActions(const ExpressionActionsSettings & settings)
    {
        /// In rare situations this function can be called a few times (e.g. ActonsDAG was clonned).
        if (!expression_actions)
            expression_actions = std::make_shared<ExpressionActions>(actions_dag->clone(), settings);
    }

private:
    std::shared_ptr<ActionsDAG> actions_dag;
    CapturePtr capture;

    ExpressionActionsPtr expression_actions;
};

class FunctionCapture : public IFunctionBase
{
public:
    using CapturePtr = ExecutableFunctionCapture::CapturePtr;

    FunctionCapture(
        std::shared_ptr<ActionsDAG> actions_dag_,
        CapturePtr capture_,
        DataTypePtr return_type_,
        String name_)
        : actions_dag(std::move(actions_dag_))
        , capture(std::move(capture_))
        , return_type(std::move(return_type_))
        , name(std::move(name_))
    {
    }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override { return capture->captured_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionCapture>(actions_dag, capture);
    }

    const ExecutableFunctionCapture::Capture & getCapture() const { return *capture; }
    const ActionsDAG & getAcionsDAG() const { return *actions_dag; }

private:
    std::shared_ptr<ActionsDAG> actions_dag;
    CapturePtr capture;
    DataTypePtr return_type;
    String name;
};

class FunctionCaptureOverloadResolver : public IFunctionOverloadResolver
{
public:
    using Capture = ExecutableFunctionCapture::Capture;
    using CapturePtr = ExecutableFunctionCapture::CapturePtr;

    FunctionCaptureOverloadResolver(
            std::shared_ptr<ActionsDAG> actions_dag_,
            const Names & captured_names,
            const NamesAndTypesList & lambda_arguments,
            const DataTypePtr & function_return_type,
            const String & expression_return_name)
        : actions_dag(std::move(actions_dag_))
    {
        /// Check that expression does not contain unusual actions that will break columns structure.
        if (actions_dag->hasArrayJoin())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression with arrayJoin or other unusual action cannot be captured");

        std::unordered_map<std::string, DataTypePtr> arguments_map;

        for (const auto * input : actions_dag->getInputs())
            arguments_map[input->result_name] = input->result_type;

        DataTypes captured_types;
        captured_types.reserve(captured_names.size());

        for (const auto & captured_name : captured_names)
        {
            auto it = arguments_map.find(captured_name);
            if (it == arguments_map.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Lambda captured argument {} not found in required columns.", captured_name);

            captured_types.push_back(it->second);
            arguments_map.erase(it);
        }

        DataTypes argument_types;
        argument_types.reserve(lambda_arguments.size());
        for (const auto & lambda_argument : lambda_arguments)
            argument_types.push_back(lambda_argument.type);

        return_type = std::make_shared<DataTypeFunction>(argument_types, function_return_type);

        name = "Capture[" + toString(captured_types) + "](" + toString(argument_types) + ") -> "
               + function_return_type->getName();

        capture = std::make_shared<Capture>(Capture{
                .captured_names = captured_names,
                .captured_types = std::move(captured_types),
                .lambda_arguments = lambda_arguments,
                .return_name = expression_return_name,
                .return_type = function_return_type,
        });
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForNulls() const override { return false; }
    /// See comment in ExecutableFunctionCapture.
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return return_type; }
    size_t getNumberOfArguments() const override { return capture->captured_types.size(); }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &, const DataTypePtr &) const override
    {
        return std::make_unique<FunctionCapture>(actions_dag, capture, return_type, name);
    }

private:
    std::shared_ptr<ActionsDAG> actions_dag;
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
