#include "UserDefinedSQLFunctionVisitor.h"

#include <unordered_map>
#include <unordered_set>
#include <stack>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Interpreters/UserDefinedSQLFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

void UserDefinedSQLFunctionMatcher::visit(ASTPtr & ast, Data &)
{
    auto * function = ast->as<ASTFunction>();
    if (!function)
        return;

    std::unordered_set<std::string> udf_in_replace_process;
    auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process);
    if (replace_result)
        ast = replace_result;
}

bool UserDefinedSQLFunctionMatcher::needChildVisit(const ASTPtr &, const ASTPtr &)
{
    return true;
}

ASTPtr UserDefinedSQLFunctionMatcher::tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process)
{
    if (udf_in_replace_process.find(function.name) != udf_in_replace_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive function call detected during function call {}",
            function.name);

    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function.name);
    if (!user_defined_function)
        return nullptr;

    const auto & function_arguments_list = function.children.front()->as<ASTExpressionList>();
    auto & function_arguments = function_arguments_list->children;

    const auto & create_function_query = user_defined_function->as<ASTCreateFunctionQuery>();
    auto & function_core_expression = create_function_query->function_core->children.front();

    const auto & identifiers_expression_list = function_core_expression->children.front()->children.front()->as<ASTExpressionList>();
    const auto & identifiers_raw = identifiers_expression_list->children;

    if (function_arguments.size() != identifiers_raw.size())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Function {} expects {} arguments actual arguments {}",
            create_function_query->getFunctionName(),
            identifiers_raw.size(),
            function_arguments.size());

    std::unordered_map<std::string, ASTPtr> identifier_name_to_function_argument;

    auto id_it = identifiers_raw.begin();
    auto args_it = function_arguments.begin();
    for (; id_it != identifiers_raw.end(); ++id_it, ++args_it)
    {
        const auto & identifier = (*id_it)->as<ASTIdentifier>();
        const auto & function_argument = *args_it;
        const auto & identifier_name = identifier->name();

        identifier_name_to_function_argument.emplace(identifier_name, function_argument);
    }

    auto [it, _] = udf_in_replace_process.emplace(function.name);

    auto function_body_to_update = (*++function_core_expression->children.begin())->clone();

    auto expression_list = std::make_shared<ASTExpressionList>();
    expression_list->children.emplace_back(std::move(function_body_to_update));

    std::stack<ASTPtr> ast_nodes_to_update;
    ast_nodes_to_update.push(expression_list);

    while (!ast_nodes_to_update.empty())
    {
        auto ast_node_to_update = ast_nodes_to_update.top();
        ast_nodes_to_update.pop();

        for (auto & child : ast_node_to_update->children)
        {
            if (auto * inner_function = child->as<ASTFunction>())
            {
                auto replace_result = tryToReplaceFunction(*inner_function, udf_in_replace_process);
                if (replace_result)
                    child = replace_result;
            }

            auto identifier_name_opt = tryGetIdentifierName(child);
            if (identifier_name_opt)
            {
                auto function_argument_it = identifier_name_to_function_argument.find(*identifier_name_opt);

                if (function_argument_it == identifier_name_to_function_argument.end())
                    continue;

                auto child_alias = child->tryGetAlias();
                child = function_argument_it->second->clone();

                if (!child_alias.empty())
                    child->setAlias(child_alias);

                continue;
            }

            ast_nodes_to_update.push(child);
        }
    }

    udf_in_replace_process.erase(it);

    function_body_to_update = expression_list->children.front();

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}

}
