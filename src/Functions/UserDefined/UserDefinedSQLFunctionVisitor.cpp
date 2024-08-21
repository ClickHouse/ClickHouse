#include "UserDefinedSQLFunctionVisitor.h"

#include <unordered_map>
#include <unordered_set>
#include <stack>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include "Parsers/ASTColumnDeclaration.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

void UserDefinedSQLFunctionVisitor::visit(ASTPtr & ast)
{
    if (!ast)
    {
        chassert(false);
        return;
    }

    const auto visit_child_with_shared_ptr = [&](ASTPtr & child)
    {
        if (!child)
            return;

        auto * old_value = child.get();
        visit(child);

        // child did not change
        if (old_value == child.get())
            return;

        // child changed, we need to modify it in the list of children of the parent also
        for (auto & current_child : ast->children)
        {
            if (current_child.get() == old_value)
                current_child = child;
        }
    };

    if (auto * col_decl = ast->as<ASTColumnDeclaration>())
    {
        visit_child_with_shared_ptr(col_decl->default_expression);
        visit_child_with_shared_ptr(col_decl->ttl);
        return;
    }

    if (auto * storage = ast->as<ASTStorage>())
    {
        const auto visit_child = [&](IAST * & child)
        {
            if (!child)
                return;

            if (const auto * function = child->template as<ASTFunction>())
            {
                std::unordered_set<std::string> udf_in_replace_process;
                auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process);
                if (replace_result)
                    ast->setOrReplace(child, replace_result);
            }

            visit(child);
        };

        visit_child(storage->partition_by);
        visit_child(storage->primary_key);
        visit_child(storage->order_by);
        visit_child(storage->sample_by);
        visit_child(storage->ttl_table);

        return;
    }

    if (auto * alter = ast->as<ASTAlterCommand>())
    {
        visit_child_with_shared_ptr(alter->col_decl);
        visit_child_with_shared_ptr(alter->column);
        visit_child_with_shared_ptr(alter->partition);
        visit_child_with_shared_ptr(alter->order_by);
        visit_child_with_shared_ptr(alter->sample_by);
        visit_child_with_shared_ptr(alter->index_decl);
        visit_child_with_shared_ptr(alter->index);
        visit_child_with_shared_ptr(alter->constraint_decl);
        visit_child_with_shared_ptr(alter->constraint);
        visit_child_with_shared_ptr(alter->projection_decl);
        visit_child_with_shared_ptr(alter->projection);
        visit_child_with_shared_ptr(alter->predicate);
        visit_child_with_shared_ptr(alter->update_assignments);
        visit_child_with_shared_ptr(alter->values);
        visit_child_with_shared_ptr(alter->ttl);
        visit_child_with_shared_ptr(alter->select);

        return;
    }

    if (const auto * function = ast->template as<ASTFunction>())
    {
        std::unordered_set<std::string> udf_in_replace_process;
        auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process);
        if (replace_result)
            ast = replace_result;
    }

    for (auto & child : ast->children)
        visit(child);
}

void UserDefinedSQLFunctionVisitor::visit(IAST * ast)
{
    if (!ast)
        return;

    for (auto & child : ast->children)
        visit(child);
}

ASTPtr UserDefinedSQLFunctionVisitor::tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process)
{
    if (udf_in_replace_process.find(function.name) != udf_in_replace_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive function call detected during function call {}",
            function.name);

    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function.name);
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
            create_function_query->getFunctionName(),
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

    auto [it, _] = udf_in_replace_process.emplace(function.name);

    auto function_body_to_update = function_core_expression->children.at(1)->clone();

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

    function_body_to_update = expression_list->children[0];

    auto function_alias = function.tryGetAlias();

    if (!function_alias.empty())
        function_body_to_update->setAlias(function_alias);

    return function_body_to_update;
}

}
