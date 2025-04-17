#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/joinCost.h>
#include <Core/Joins.h>
#include <ranges>
#include <memory>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Object.h>
#include <Common/StringUtils.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>

/*
 * This file contains helper functions for debugging and testing join optimization.
 * These functions allow developers to:
 * - Hard-code specific join orders for testing alternative execution plans
 * - Override table statistics through query parameters for performance testing
 *
 * Note: These functions are intended for development and testing purposes only,
 * not for production use.
 */
namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/* Read dummy stats from query parameter
 * The parameter should be a JSON object with the following structure:
 * SET param__internal_join_table_stat_hints = '{
 *   "table_name": { "cardinality": 1000, "distinct_keys": { "column_name": 100, ... } },
 *   ...
 * }';
 */
RelationStats getDummyStats(ContextPtr context, const String & table_name)
{
    constexpr auto param_name = "_internal_join_table_stat_hints";

    const auto & query_params = context->getQueryParameters();
    auto it = query_params.find(param_name);
    if (it == query_params.end())
        return {};

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var result = parser.parse(it->second);
        auto object = result.extract<Poco::JSON::Object::Ptr>();
        if (!object)
            return {};

        if (!object->has(table_name))
            return {};

        auto stat_object = object->getObject(table_name);
        if (!stat_object)
            return {};

        RelationStats stats;
        stats.table_name = table_name;

        if (stat_object->has("cardinality"))
            stats.estimated_rows = stat_object->getValue<UInt64>("cardinality");

        if (stat_object->isObject("distinct_keys"))
        {
            auto distinct_keys = stat_object->getObject("distinct_keys");
            for (const auto & [key, value] : *distinct_keys)
                stats.column_stats[key].num_distinct_values = value.convert<UInt64>();
        }
        LOG_WARNING(getLogger("optimizeJoin"),
            "Got dummy join stats for table '{}' from '{}' query parameter, it's supposed to be used only for testing, do not use it in production",
            table_name, param_name);
        return stats;
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(getLogger("optimizeJoin"), "Failed to parse '{}': {}", param_name, e.displayText());
        return {};
    }
}

/// Transform expression to postfix notation
/// For example: `(A * B) * (C * D) * E` -> `A B * C D * * E *`
std::vector<std::string_view> parseInfixOrderToPostfix(std::string_view infix_order)
{
    std::vector<std::string_view> postfix_order;
    std::stack<std::string_view> stack;
    size_t operand_start = std::string_view::npos;

    for (size_t i = 0; i < infix_order.length(); ++i)
    {
        char c = infix_order[i];

        if (isAlphaNumericASCII(c))
        {
            if (operand_start == std::string_view::npos)
                operand_start = i;
            continue;
        }

        if (operand_start != std::string_view::npos)
        {
            postfix_order.push_back(infix_order.substr(operand_start, i - operand_start));
            operand_start = std::string_view::npos;
        }

        if (c == '*')
        {
            while (!stack.empty() && stack.top() != "(")
            {
                postfix_order.push_back(stack.top());
                stack.pop();
            }
            stack.push(infix_order.substr(i, 1));
        }
        else if (c == '(')
        {
            stack.push(infix_order.substr(i, 1));
        }
        else if (c == ')')
        {
            while (!stack.empty() && stack.top() != "(")
            {
                postfix_order.push_back(stack.top());
                stack.pop();
            }
            if (stack.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mismatched parentheses in expression");
            stack.pop();
        }
        else if (!isWhitespaceASCII(c))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid character in expression: {}", c);
    }

    if (operand_start != std::string_view::npos)
        postfix_order.push_back(infix_order.substr(operand_start));

    while (!stack.empty())
    {
        if (stack.top() == "(")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mismatched parentheses in expression");
        postfix_order.push_back(stack.top());
        stack.pop();
    }

    return postfix_order;
}

}
