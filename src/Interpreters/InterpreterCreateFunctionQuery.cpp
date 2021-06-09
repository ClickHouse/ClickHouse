#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterCreateFunctionQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Functions/FunctionFactory.h>
#include <Parsers/ASTIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
}

BlockIO InterpreterCreateFunctionQuery::execute()
{
    FunctionNameNormalizer().visit(query_ptr.get());
    auto & create_function_query = query_ptr->as<ASTCreateFunctionQuery &>();
    validateFunction(create_function_query.function_core);
    FunctionFactory::instance().registerUserDefinedFunction(create_function_query);
    return {};
}

void InterpreterCreateFunctionQuery::validateFunction(ASTPtr function)
{
    const auto * args_tuple = function->as<ASTFunction>()->arguments->children.at(0)->as<ASTFunction>();
    std::unordered_set<String> arguments;
    for (const auto & argument : args_tuple->arguments->children)
        arguments.insert(argument->as<ASTIdentifier>()->name());

    std::vector<String> identifiers_in_body;
    ASTPtr function_body = function->as<ASTFunction>()->children.at(0)->children.at(1);
    getIdentifiers(function_body, identifiers_in_body);

    for (const auto & identifier : identifiers_in_body)
    {
        if (!arguments.contains(identifier))
        {
            WriteBufferFromOwnString s;
            s << "Identifier '" << identifier << "' does not exist in arguments";
            throw Exception(s.str(), ErrorCodes::UNKNOWN_IDENTIFIER);
        }
    }
}

void InterpreterCreateFunctionQuery::getIdentifiers(ASTPtr node, std::vector<String> & identifiers)
{
    for (const auto & child : node->children)
    {
        auto identifier_name_opt = tryGetIdentifierName(child);
        if (identifier_name_opt)
            identifiers.push_back(identifier_name_opt.value());

        getIdentifiers(child, identifiers);
    }
}

}
