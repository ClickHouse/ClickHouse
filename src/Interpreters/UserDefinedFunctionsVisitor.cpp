#include "UserDefinedFunctionsVisitor.h"

#include <unordered_map>
#include <stack>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/UserDefinedFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

void UserDefinedFunctionsMatcher::visit(ASTPtr & ast, Data & data)
{
    auto * function = ast->as<ASTFunction>();
    if (!function)
        return;

    auto result = tryToReplaceFunction(*function);
    if (result)
    {
        ast = result;
        visit(ast, data);
    }
}

bool UserDefinedFunctionsMatcher::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

ASTPtr UserDefinedFunctionsMatcher::tryToReplaceFunction(const ASTFunction & function)
{
    auto user_defined_function = UserDefinedFunctionFactory::instance().tryGet(function.name);
    if (!user_defined_function)
        return nullptr;

    const auto & function_arguments_list = function.children.at(0)->as<ASTExpressionList>();
    auto & function_arguments = function_arguments_list->children;

    const auto & create_function_query = user_defined_function->as<ASTCreateFunctionQuery>();
    auto & function_core_expression = create_function_query->function_core->children.at(0);

    const auto & identifiers_expression_list = function_core_expression->children.at(0)->children.at(0)->as<ASTExpressionList>();
    const auto & identifiers_raw = identifiers_expression_list->children;

    if (function_arguments.size() != identifiers_raw.size())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Function {} expects {} arguments actual arguments {}",
            create_function_query->function_name,
            identifiers_raw.size(),
            function_arguments.size());

    std::unordered_map<std::string, ASTPtr> identifier_name_to_function_argument;

    for (size_t parameter_index = 0; parameter_index < identifiers_raw.size(); ++parameter_index)
    {
        const auto & identifier = identifiers_raw[parameter_index]->as<ASTIdentifier>();
        const auto & function_argument = function_arguments[parameter_index];
        const auto & identifier_name = identifier->name();

        identifier_name_to_function_argument.emplace(identifier_name, function_argument);
    }

    auto function_body_to_update = function_core_expression->children.at(1)->clone();

    std::stack<ASTPtr> ast_nodes_to_update;
    ast_nodes_to_update.push(function_body_to_update);

    while (!ast_nodes_to_update.empty())
    {
        auto ast_node_to_update = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        for (auto & child : ast_node_to_update->children)
        {
            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
            {
                auto function_argument_it = identifier_name_to_function_argument.find(*identifier_name_opt);
                assert(function_argument_it != identifier_name_to_function_argument.end());

                child = function_argument_it->second->clone();
                continue;
            }

            ast_nodes_to_update.push(child);
        }
    }

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}

}
