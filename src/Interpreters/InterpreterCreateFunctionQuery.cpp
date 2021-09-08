#include <Access/ContextAccess.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/UserDefinedObjectsLoader.h>
#include <Interpreters/UserDefinedFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
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

    auto & function_name = create_function_query->function_name;
    validateFunction(create_function_query->function_core, function_name);

    UserDefinedFunctionFactory::instance().registerFunction(function_name, query_ptr);

    if (!is_internal)
    {
        try
        {
            UserDefinedObjectsLoader::instance().storeObject(current_context, UserDefinedObjectType::Function, function_name, *query_ptr);
        }
        catch (Exception & exception)
        {
            UserDefinedFunctionFactory::instance().unregisterFunction(function_name);
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
    std::unordered_set<String> identifiers_in_body = getIdentifiers(function_body);

    for (const auto & identifier : identifiers_in_body)
    {
        if (!arguments.contains(identifier))
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Identifier {} does not exist in arguments", backQuote(identifier));
    }

    validateFunctionRecursiveness(function_body, name);
}

std::unordered_set<String> InterpreterCreateFunctionQuery::getIdentifiers(ASTPtr node)
{
    std::unordered_set<String> identifiers;

    std::stack<ASTPtr> ast_nodes_to_process;
    ast_nodes_to_process.push(node);

    while (!ast_nodes_to_process.empty())
    {
        auto ast_node_to_process = ast_nodes_to_process.top();
        ast_nodes_to_process.pop();

        for (const auto & child : ast_node_to_process->children)
        {
            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
                identifiers.insert(identifier_name_opt.value());

            ast_nodes_to_process.push(child);
        }
    }

    return identifiers;
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
