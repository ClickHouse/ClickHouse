#include <Interpreters/RewriteRulesASTTraversal.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTAlterRewriteRuleQuery.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <functional>
#include <queue>
#include <unordered_map>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool query_rules;
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
}

namespace ErrorCodes
{
    extern const int REWRITE_RULE_REJECTION;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE;
}

bool astTraversal(ASTPtr &ast, ContextPtr context)
{
    const auto& settings = context->getSettingsRef();
    if (!ast || !settings[Setting::query_rules])
    {
        return false;
    }

    auto original_query = ast->formatForLogging();
    Array applied_rules;
    for (const auto& [name, rule] : RewriteRules::instance().getAll())
    {
        const auto& query_rule = rule->getCreateQuery();
        std::queue<ASTPtr> queue_query;
        std::queue<ASTPtr> queue_rule;
        std::unordered_map<String, ASTPtr> matching_map;
        queue_query.push(ast);
        queue_rule.push(query_rule.source_query);
        bool is_template = true;
        while (!queue_query.empty())
        {
            if (queue_query.size() != queue_rule.size())
            {
                is_template = false;
                break;
            }
            auto top1 = queue_query.front();
            auto top2 = queue_rule.front();
            queue_query.pop();
            queue_rule.pop();
            if (top1->getTreeHash(true) != top2->getTreeHash(true))
            {
                auto hash1 = top1->getCurrentNodeHash(true);
                auto hash2 = top2->getCurrentNodeHash(true);
                // Second check exists because there are situations when ASTQueryParameter is embedded into ASTExpressionList
                if (hash1 != hash2 || (top2->as<ASTExpressionList>() && top2->children.size() == 1 && top2->children[0]->as<ASTQueryParameter>()))
                {
                    auto* query_parameter = top2->as<ASTQueryParameter>();
                    /// A query parameter can be embedded as the sole child of an
                    /// `ASTExpressionList` (for example the only projection in
                    /// `SELECT {x:String}`). In that case both `top1` and `top2` are the
                    /// wrapper, so unwrap `top2` to reach the parameter.
                    const bool wrapped_parameter = hash1 == hash2 && !top2->children.empty();
                    if (wrapped_parameter)
                    {
                        query_parameter = top2->children[0]->as<ASTQueryParameter>();
                    }
                    if (!query_parameter)
                    {
                        is_template = false;
                        break;
                    }
                    auto query_parameter_type = query_parameter->type;
                    trimRight(query_parameter_type);
                    trimLeft(query_parameter_type);
                    /// The query-side node to capture. For a wrapped parameter `top1` is the
                    /// `ASTExpressionList` wrapper: an `ExpressionList` placeholder captures
                    /// the whole wrapper, but a scalar (`String`/`Int`), `Expression` or
                    /// `Subquery` placeholder must capture the single inner node. Otherwise
                    /// the wrapper is bound, so either the `ASTLiteral` check below fails (and
                    /// `SELECT {x:String}` would not match `SELECT 'hello'`) or `applyRule`
                    /// substitutes an extra `ASTExpressionList` layer into the resulting query.
                    ASTPtr match_node = top1;
                    if (wrapped_parameter && query_parameter_type != "ExpressionList"
                        && top1->as<ASTExpressionList>() && top1->children.size() == 1)
                    {
                        match_node = top1->children[0];
                    }
                    auto add_to_matching_map = [&](ASTPtr cloned_ast)
                    {
                        if (matching_map.contains(query_parameter->name))
                        {
                            throw Exception(
                                ErrorCodes::REWRITE_RULE_DUPLICATED_QUERY_PARAMETER,
                                "Query parameter duplicate in rewrite rule template: {}\n",
                                query_parameter->name
                            );
                        }
                        matching_map.emplace(query_parameter->name, std::move(cloned_ast));
                    };
                    if (auto* literal = match_node->as<ASTLiteral>();
                        literal && ((query_parameter_type == "String" && literal->value.getType() == Field::Types::Which::String)
                        || (query_parameter_type == "Int" && Field::isDecimal(literal->value.getType()))
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt128)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt256)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::UInt64)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int128)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int256)
                        || (query_parameter_type == "Int" && literal->value.getType() == Field::Types::Which::Int64)))
                    {
                        add_to_matching_map(literal->clone());
                    }
                    else if (query_parameter_type == "Expression")
                    {
                        /// An `Expression` placeholder captures a single arbitrary expression subtree.
                        add_to_matching_map(match_node->clone());
                    }
                    else if (auto* expression = match_node->as<ASTExpressionList>();
                        expression && query_parameter_type == "ExpressionList")
                    {
                        add_to_matching_map(expression->clone());
                    }
                    else if (auto* subquery = match_node->as<ASTSubquery>();
                        subquery && query_parameter_type == "Subquery")
                    {
                        add_to_matching_map(subquery->clone());
                    }
                    else
                    {
                        is_template = false;
                        break;
                    }
                    continue;
                }
                for (const auto& child : top1->children)
                {
                    queue_query.push(child);
                }
                for (const auto& child : top2->children)
                {
                    queue_rule.push(child);
                }
            }
            else
            {
                /// Trees are structurally identical at this node. If the rule template
                /// contains a query parameter at this position (because the incoming
                /// query is also parameterized with the same name and type), the hashes
                /// match and the branch above does not record a binding. Without a
                /// binding, applyRule throws REWRITE_RULE_UNKNOWN_QUERY_PARAMETER when
                /// substituting the parameter in the resulting query template.
                auto * query_parameter = top2->as<ASTQueryParameter>();
                /// When the parameter is wrapped in a single-child `ASTExpressionList`,
                /// both `top1` and `top2` are the wrapper. We must bind the inner node,
                /// not the wrapper, otherwise `applyRule` substitutes an extra
                /// `ASTExpressionList` layer into the resulting template.
                ASTPtr bound_node = top1;
                if (!query_parameter
                    && top2->as<ASTExpressionList>() && top2->children.size() == 1)
                {
                    query_parameter = top2->children[0]->as<ASTQueryParameter>();
                    if (query_parameter && top1->children.size() == 1)
                        bound_node = top1->children[0];
                }
                if (query_parameter)
                {
                    if (matching_map.contains(query_parameter->name))
                    {
                        throw Exception(
                            ErrorCodes::REWRITE_RULE_DUPLICATED_QUERY_PARAMETER,
                            "Query parameter duplicate in rewrite rule template: {}\n",
                            query_parameter->name
                        );
                    }
                    matching_map.emplace(query_parameter->name, bound_node->clone());
                    continue;
                }
                /// Deeper parameters may exist below — continue BFS into children.
                for (const auto& child : top1->children)
                {
                    queue_query.push(child);
                }
                for (const auto& child : top2->children)
                {
                    queue_rule.push(child);
                }
            }
        }
        if (is_template)
        {
            applied_rules.push_back(name);
            /// A `REJECT WITH` rule throws from `applyRule` before the end-of-function
            /// logging is reached, so the rejection would otherwise never appear in
            /// `system.query_rules_log`. Record the match first (with an empty
            /// `resulting_query`, which marks a rejection because a rewrite always
            /// produces a non-empty resulting query) so operators can audit which rule
            /// rejected a query.
            if (query_rule.reject())
                RewriteRules::instance().addLog(original_query, applied_rules, /* resulting_query */ "");
            applyRule(ast, rule, matching_map);
        }
    }

    if (!applied_rules.empty())
    {
        RewriteRules::instance().addLog(original_query, applied_rules, ast->formatForLogging());
    }

    return true;
}


void checkRewriteRuleTemplateLimits(const ASTPtr & source_query, const ASTPtr & resulting_query, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    std::function<void(const ASTPtr &)> check = [&](const ASTPtr & ast)
    {
        if (!ast)
            return;
        if (settings[Setting::max_ast_depth])
            ast->checkDepth(settings[Setting::max_ast_depth]);
        if (settings[Setting::max_ast_elements])
            ast->checkSize(settings[Setting::max_ast_elements]);

        /// A template can itself be a `CREATE RULE` / `ALTER RULE` whose own source and
        /// result templates are stored outside `IAST::children`, so the `checkDepth` /
        /// `checkSize` walk above never reaches them. Descend into nested rule DDL
        /// templates explicitly, otherwise an oversized or very deep inner template
        /// (e.g. `CREATE RULE outer AS (CREATE RULE inner AS (SELECT <huge>) REWRITE TO
        /// (SELECT 1)) REWRITE TO (SELECT 1)`) would bypass the limits and be persisted.
        if (const auto * create_rule = ast->as<ASTCreateRewriteRuleQuery>())
        {
            check(create_rule->source_query);
            check(create_rule->resulting_query);
        }
        else if (const auto * alter_rule = ast->as<ASTAlterRewriteRuleQuery>())
        {
            check(alter_rule->source_query);
            check(alter_rule->resulting_query);
        }
    };
    check(source_query);
    check(resulting_query);
}


void applyRule(ASTPtr &ast, RewriteRuleObjectPtr rule, std::unordered_map<String, ASTPtr>& matching_map)
{
    const auto& query_rule = rule->getCreateQuery();
    if (query_rule.rewrite())
    {
        auto resulting_query = query_rule.resulting_query->clone();
        std::queue<ASTPtr> queue;
        queue.push(resulting_query);
        while (!queue.empty())
        {
            auto top = queue.front();
            queue.pop();

            /// Whether substituting an `ExpressionList` capture here means splicing its
            /// items into this node's child list (projection lists, function-argument
            /// lists) rather than inserting the captured list as a single child.
            const bool parent_is_list = top->as<ASTExpressionList>() != nullptr;

            ASTs new_children;
            new_children.reserve(top->children.size());
            for (auto & child : top->children)
            {
                auto * query_parameter = child->as<ASTQueryParameter>();
                if (!query_parameter)
                {
                    /// Not a placeholder: keep the node and descend into it so that
                    /// placeholders nested deeper are substituted too.
                    queue.push(child);
                    new_children.push_back(child);
                    continue;
                }

                auto it = matching_map.find(query_parameter->name);
                if (it == matching_map.end())
                {
                    throw Exception(
                        ErrorCodes::REWRITE_RULE_UNKNOWN_QUERY_PARAMETER,
                        "Resulting rewrite rule template contains unknown query parameter: {}\n",
                        query_parameter->name
                    );
                }

                /// Clone instead of moving so the same parameter can be substituted in
                /// multiple positions of the resulting query template.
                ASTPtr captured = it->second->clone();

                if (auto * captured_list = captured->as<ASTExpressionList>())
                {
                    /// An `ExpressionList` capture holds the matched `ASTExpressionList`.
                    /// Inside another `ASTExpressionList` its items must be spliced into
                    /// the parent list, not nested as one child — otherwise the analyzer
                    /// builds a single projection entry from the whole captured list (so
                    /// `SELECT {l:ExpressionList}, 3` over `SELECT 1, 2` would yield
                    /// `[ASTExpressionList(1, 2), 3]` rather than `[1, 2, 3]`).
                    if (!parent_is_list)
                        throw Exception(
                            ErrorCodes::REWRITE_RULE_UNSUPPORTED_QUERY_PARAMETER_TYPE,
                            "Resulting rewrite rule template substitutes the ExpressionList query "
                            "parameter `{}` where a single expression is expected\n",
                            query_parameter->name
                        );

                    for (auto & item : captured_list->children)
                        new_children.push_back(item);
                }
                else
                {
                    new_children.push_back(std::move(captured));
                }
            }
            top->children = std::move(new_children);
        }
        ast = std::move(resulting_query);
    } else if (query_rule.reject())
    {
        throw Exception(
            ErrorCodes::REWRITE_RULE_REJECTION,
            "Query was rejected by {} with message: {}\n",
            query_rule.rule_name, query_rule.reject_message
        );
    }
}

};
