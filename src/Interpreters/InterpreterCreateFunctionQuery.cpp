#include <Interpreters/InterpreterCreateFunctionQuery.h>

#include <stack>

#include <Access/ContextAccess.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
    extern const int UNSUPPORTED_METHOD;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_FUNCTION);

    FunctionNameNormalizer().visit(query_ptr.get());
    auto * create_function_query = query_ptr->as<ASTCreateFunctionQuery>();

    if (!create_function_query)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected CREATE FUNCTION query");

    auto & user_defined_function_factory = UserDefinedSQLFunctionFactory::instance();

    auto & function_name = create_function_query->function_name;

    bool if_not_exists = create_function_query->if_not_exists;
    bool replace = create_function_query->or_replace;

    create_function_query->if_not_exists = false;
    create_function_query->or_replace = false;

    if (if_not_exists && user_defined_function_factory.tryGet(function_name) != nullptr)
        return {};

    validateFunction(create_function_query->function_core, function_name);

    user_defined_function_factory.registerFunction(function_name, query_ptr, replace);

    if (persist_function)
    {
        try
        {
            UserDefinedSQLObjectsLoader::instance().storeObject(current_context, UserDefinedSQLObjectType::Function, function_name, *query_ptr, replace);
        }
        catch (Exception & exception)
        {
            user_defined_function_factory.unregisterFunction(function_name);
            exception.addMessage(fmt::format("while storing user defined function {} on disk", backQuote(function_name)));
            throw;
        }
    }

    return {};
}

void InterpreterCreateFunctionQuery::validateFunction(ASTPtr function, const String & name)
{
    const auto * args_tuple = function->as<ASTFunction>()->arguments->children.at(0)->as<ASTFunction>();
    std::unordered_set<String> arguments;
    for (const auto & argument : args_tuple->arguments->children)
    {
        const auto & argument_name = argument->as<ASTIdentifier>()->name();
        auto [_, inserted] = arguments.insert(argument_name);
        if (!inserted)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Identifier {} already used as function parameter", argument_name);
    }

    ASTPtr function_body = function->as<ASTFunction>()->children.at(0)->children.at(1);
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
