#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/UserDefinedObjectsLoader.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int CANNOT_CREATE_RECURSIVE_FUNCTION;
    // extern const int UNSUPPORTED_OPERATION;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    getContext()->checkAccess(AccessType::CREATE_FUNCTION);
    FunctionNameNormalizer().visit(query_ptr.get());
    auto * create_function_query = query_ptr->as<ASTCreateFunctionQuery>();

    // if (!create_function_query)
    //     throw Exception(ErrorCodes::UNSUPPORTED_OPERATION, "Expected CREATE FUNCTION query");

    auto & function_name = create_function_query->function_name;
    validateFunction(create_function_query->function_core, function_name);

    if (is_internal)
    {
        FunctionFactory::instance().registerUserDefinedFunction(*create_function_query);
    }
    else
    {
        try
        {
            UserDefinedObjectsLoader::instance().storeObject(getContext(), UserDefinedObjectType::Function, function_name, *query_ptr);
            FunctionFactory::instance().registerUserDefinedFunction(*create_function_query);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while storing user defined function {} on disk", backQuote(function_name)));
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
        arguments.insert(argument->as<ASTIdentifier>()->name());

    std::set<String> identifiers_in_body;
    ASTPtr function_body = function->as<ASTFunction>()->children.at(0)->children.at(1);
    getIdentifiers(function_body, identifiers_in_body);

    for (const auto & identifier : identifiers_in_body)
    {
        if (!arguments.contains(identifier))
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Identifier {} does not exist in arguments", backQuote(identifier));
    }

    validateFunctionRecursiveness(function_body, name);
}

void InterpreterCreateFunctionQuery::getIdentifiers(ASTPtr node, std::set<String> & identifiers)
{
    for (const auto & child : node->children)
    {
        auto identifier_name_opt = tryGetIdentifierName(child);
        if (identifier_name_opt)
            identifiers.insert(identifier_name_opt.value());

        getIdentifiers(child, identifiers);
    }
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
