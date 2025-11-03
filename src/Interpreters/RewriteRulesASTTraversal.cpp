#include <Interpreters/RewriteRulesASTTraversal.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <queue>
#include <unordered_map>
#include <Interpreters/Context.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool query_rules;
}

namespace ErrorCodes
{
    extern const int REWRITE_RULE_REJECTION;
    extern const int REWRITE_RULE_DUPLICATED_QUERY_PARAMETER;
    extern const int REWRITE_RULE_UNKNOWN_QUERY_PARAMETER;
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
                    if (hash1 == hash2 && !top2->children.empty())
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
                    if (auto* literal = top1->as<ASTLiteral>();
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
                    } else
                        if (auto* expression = top1->as<ASTExpressionList>();
                            expression && (query_parameter_type == "Expression" || query_parameter_type == "ExpressionList"))
                        {
                            add_to_matching_map(expression->clone());
                    } else
                        if (auto* subquery = top1->as<ASTSubquery>();
                            subquery && query_parameter_type == "Subquery")
                        {
                            add_to_matching_map(subquery->clone());
                    } else
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
        }
        if (is_template)
        {
            applyRule(ast, rule, matching_map);
            applied_rules.push_back(name);
        }
    }

    if (!applied_rules.empty())
    {
        RewriteRules::instance().addLog(original_query, applied_rules, ast->formatForLogging());
    }

    return true;
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
            for (auto& child : top->children)
            {
                if (auto* query_parameter = child->as<ASTQueryParameter>();
                    query_parameter)
                {
                    if (auto it = matching_map.find(query_parameter->name);
                        it != matching_map.end())
                    {
                        child = std::move(it->second);
                    } else
                    {
                        throw Exception(
                            ErrorCodes::REWRITE_RULE_UNKNOWN_QUERY_PARAMETER,
                            "Resulting rewrite rule template contains unknown query parameter: {}\n",
                            query_parameter->name
                        );
                    }
                // } else if (child->children.size() == 1 && child->children[0]->as<ASTQueryParameter>()) {
                } else
                {
                    queue.push(child);
                }
            }
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
