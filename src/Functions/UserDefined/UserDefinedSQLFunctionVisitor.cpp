#include <Functions/UserDefined/UserDefinedSQLFunctionVisitor.h>

#include <stack>
#include <unordered_map>
#include <unordered_set>

#include <Core/Settings.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/MarkTableIdentifiersVisitor.h>
#include <Interpreters/QueryAliasesVisitor.h>
#include <Interpreters/QueryNormalizer.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTQualifiedAsterisk.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool skip_redundant_aliases_in_udf;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED_METHOD;
}

void UserDefinedSQLFunctionVisitor::visit(ASTPtr & ast, ContextPtr context_)
{
    chassert(ast);

    if (const auto * function = ast->template as<ASTFunction>())
    {
        std::unordered_set<std::string> udf_in_replace_process;
        auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process, context_);
        if (replace_result)
            ast = replace_result;
    }

    for (auto & child : ast->children)
    {
        if (!child)
            return;

        auto * old_ptr = child.get();
        visit(child, context_);
        auto * new_ptr = child.get();

        /// Some AST classes have naked pointers to children elements as members.
        /// We have to replace them if the child was replaced.
        if (new_ptr != old_ptr)
            ast->updatePointerToChild(old_ptr, new_ptr);
    }

    if (const auto * function = ast->template as<ASTFunction>())
    {
        std::unordered_set<std::string> udf_in_replace_process;
        auto replace_result = tryToReplaceFunction(*function, udf_in_replace_process, context_);
        if (replace_result)
            ast = replace_result;
    }
}

void UserDefinedSQLFunctionVisitor::visit(IAST * ast, ContextPtr context_)
{
    if (!ast)
        return;

    for (auto & child : ast->children)
        visit(child, context_);
}

namespace
{
bool isVariadic(const ASTPtr & arg)
{
    return arg->as<ASTAsterisk>() || arg->as<ASTQualifiedAsterisk>() || arg->as<ASTColumnsRegexpMatcher>()
        || arg->as<ASTColumnsListMatcher>() || arg->as<ASTQualifiedColumnsRegexpMatcher>() || arg->as<ASTQualifiedColumnsListMatcher>();
}
}

ASTPtr UserDefinedSQLFunctionVisitor::tryToReplaceFunction(const ASTFunction & function, std::unordered_set<std::string> & udf_in_replace_process, ContextPtr context_)
{
    if (udf_in_replace_process.contains(function.name))
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

    for (const auto & arg : function_arguments)
    {
        if (isVariadic(arg))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "It is not possible to replace a variadic argument '{}' in UDF {}",
                arg->getColumnName(),
                function.name);
    }

    if (isVariadic(function_core_expression->children.at(1)))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "It is not possible to replace a variadic argument '{}' in UDF {}",
            function_core_expression->children.at(1)->getColumnName(),
            function.name);

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

    if (context_->getSettingsRef()[Setting::skip_redundant_aliases_in_udf])
    {
        Aliases aliases;
        QueryAliasesVisitor(aliases).visit(function_body_to_update);

        /// Mark table ASTIdentifiers with not a column marker
        MarkTableIdentifiersVisitor::Data identifiers_data{aliases};
        MarkTableIdentifiersVisitor(identifiers_data).visit(function_body_to_update);

        /// Common subexpression elimination. Rewrite rules.
        QueryNormalizer::Data normalizer_data(aliases, {}, true, QueryNormalizer::ExtractedSettings(context_->getSettingsRef()), true, false);
        QueryNormalizer(normalizer_data).visit(function_body_to_update);
    }

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
                auto replace_result = tryToReplaceFunction(*inner_function, udf_in_replace_process, context_);
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
