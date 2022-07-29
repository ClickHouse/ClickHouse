#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
    extern const int UNSUPPORTED_METHOD;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    ASTCreateFunctionQuery & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_FUNCTION);

    if (create_function_query.or_replace)
        access_rights_elements.emplace_back(AccessType::DROP_FUNCTION);

    if (!create_function_query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(access_rights_elements);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    auto & user_defined_function_factory = UserDefinedSQLFunctionFactory::instance();

    auto function_name = create_function_query.getFunctionName();

    bool if_not_exists = create_function_query.if_not_exists;
    bool replace = create_function_query.or_replace;

    create_function_query.if_not_exists = false;
    create_function_query.or_replace = false;

    validateFunction(create_function_query.function_core, function_name);
    user_defined_function_factory.registerFunction(current_context, function_name, query_ptr, replace, if_not_exists, persist_function);

    return {};
}

void InterpreterCreateFunctionQuery::validateFunction(ASTPtr function, const String & name)
{
    ASTFunction * lambda_function = function->as<ASTFunction>();

    if (!lambda_function)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected function, got: {}", function->formatForErrorMessage());

    auto & lambda_function_expression_list = lambda_function->arguments->children;

    if (lambda_function_expression_list.size() != 2)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have arguments and body");

    const ASTFunction * tuple_function_arguments = lambda_function_expression_list.front()->as<ASTFunction>();

    if (!tuple_function_arguments || !tuple_function_arguments->arguments)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid arguments");

    std::unordered_set<String> arguments;

    for (const auto & argument : tuple_function_arguments->arguments->children)
    {
        const auto * argument_identifier = argument->as<ASTIdentifier>();

        if (!argument_identifier)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda argument must be identifier");

        const auto & argument_name = argument_identifier->name();
        auto [_, inserted] = arguments.insert(argument_name);
        if (!inserted)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Identifier {} already used as function parameter", argument_name);
    }

    ASTPtr function_body = lambda_function_expression_list.back();
    if (!function_body)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Lambda must have valid function body");

    validateFunctionRecursiveness(function_body, name);
}

void InterpreterCreateFunctionQuery::validateFunctionRecursiveness(ASTPtr node, const String & function_to_create)
{
    for (const auto & child : node->children)
    {
        auto function_name_opt = tryGetFunctionName(child);
        if (function_name_opt && function_name_opt.value() == function_to_create)
            throw Exception(ErrorCodes::CANNOT_CREATE_RECURSIVE_FUNCTION, "You cannot create recursive function");

        validateFunctionRecursiveness(child, function_to_create);
    }
}
}
