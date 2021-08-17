#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/UserDefinedFunction.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

UserDefinedFunction::UserDefinedFunction(ContextPtr context_)
    : function_core(nullptr)
    , context(context_)
{}

UserDefinedFunctionPtr UserDefinedFunction::create(ContextPtr context)
{
    return std::make_shared<UserDefinedFunction>(context);
}

String UserDefinedFunction::getName() const
{
    return name;
}

ColumnPtr UserDefinedFunction::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    Block block = executeCore(arguments);

    String result_name = function_core->as<ASTFunction>()->arguments->children.at(1)->getColumnName();

    // result of function executing was inserted in the end
    return block.getColumns().back();
}

size_t UserDefinedFunction::getNumberOfArguments() const
{
    return function_core->as<ASTFunction>()->arguments->children[0]->size() - 2;
}

void UserDefinedFunction::setName(const String & name_)
{
    name = name_;
}

void UserDefinedFunction::setFunctionCore(ASTPtr function_core_)
{
    function_core = function_core_;
}

DataTypePtr UserDefinedFunction::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    Block block = executeCore(arguments);
    return block.getDataTypes().back();
}

Block UserDefinedFunction::executeCore(const ColumnsWithTypeAndName & arguments) const
{
    const auto * lambda_args_tuple = function_core->as<ASTFunction>()->arguments->children.at(0)->as<ASTFunction>();
    const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;

    NamesAndTypesList lambda_arguments;
    Block block;

    for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
    {
        auto opt_arg_name = tryGetIdentifierName(lambda_arg_asts[j]);
        if (!opt_arg_name)
            throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

        lambda_arguments.emplace_back(*opt_arg_name, arguments[j].type);
        auto column_ptr = arguments[j].column;
        if (!column_ptr)
            column_ptr = arguments[j].type->createColumnConstWithDefaultValue(1);
        block.insert({column_ptr, arguments[j].type, *opt_arg_name});
    }

    ASTPtr lambda_body = function_core->as<ASTFunction>()->children.at(0)->children.at(1);
    auto syntax_result = TreeRewriter(context).analyze(lambda_body, lambda_arguments);
    ExpressionAnalyzer analyzer(lambda_body, syntax_result, context);
    ExpressionActionsPtr actions = analyzer.getActions(false);

    actions->execute(block);
    return block;
}

}
