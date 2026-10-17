#pragma once

#include <ranges>
#include <IO/Operators.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

class ExpressionsStack
{
public:
    void push(const QueryTreeNodePtr & node)
    {
        if (node->hasAlias())
        {
            const auto & node_alias = node->getAlias();
            alias_name_to_expressions[node_alias].push_back(node);
        }

        const auto * function = node->as<FunctionNode>();
        const bool is_counted = function && isAggregateOrGroupingFunction(*function);
        if (is_counted)
            ++aggregate_or_grouping_functions_counter;

        expressions.emplace_back(node);
        expression_is_counted.push_back(is_counted);
    }

    void pop()
    {
        const auto & top_expression = expressions.back();
        const auto & top_expression_alias = top_expression->getAlias();

        if (!top_expression_alias.empty())
        {
            auto it = alias_name_to_expressions.find(top_expression_alias);
            auto & alias_expressions = it->second;
            alias_expressions.pop_back();

            if (alias_expressions.empty())
                alias_name_to_expressions.erase(it);
        }

        if (expression_is_counted.back())
            --aggregate_or_grouping_functions_counter;

        expressions.pop_back();
        expression_is_counted.pop_back();
    }

    /// A call of a lambda or SQL user defined function is not an aggregate function, even if its name is one.
    void uncountTopIfAggregateOrGroupingFunction(const IQueryTreeNode * node)
    {
        if (expressions.empty() || expressions.back().get() != node || !expression_is_counted.back())
            return;

        expression_is_counted.back() = false;
        --aggregate_or_grouping_functions_counter;
    }

    void enterAggregateFunctionArguments() { ++aggregate_or_grouping_functions_counter; }
    void leaveAggregateFunctionArguments() { --aggregate_or_grouping_functions_counter; }

    [[maybe_unused]] const QueryTreeNodePtr & getRoot() const
    {
        return expressions.front();
    }

    const QueryTreeNodePtr & getTop() const
    {
        return expressions.back();
    }

    [[maybe_unused]] const QueryTreeNodePtr & operator[](int32_t n) const
    {
        if (n < 0)
            n = static_cast<int32_t>(expressions.size()) + n - 1;
        return expressions[n];
    }

    [[maybe_unused]] bool hasExpressionWithAlias(const std::string & alias) const
    {
        return alias_name_to_expressions.contains(alias);
    }

    /** Returns true if the stack contains an aggregate or `grouping` function.
      *
      * It is used to decide whether an expression equal to a GROUP BY key must be converted
      * to Nullable when `group_by_use_nulls` is enabled. Arguments of aggregate functions are
      * computed before the nullability is applied to the keys, and arguments of the `grouping`
      * function only identify GROUP BY keys and are compared with them in their original form
      * by `GroupingFunctionsResolvePass`.
      */
    bool hasAggregateOrGroupingFunction() const
    {
        return aggregate_or_grouping_functions_counter > 0;
    }

    QueryTreeNodePtr getExpressionWithAlias(const std::string & alias) const
    {
        auto expression_it = alias_name_to_expressions.find(alias);
        if (expression_it == alias_name_to_expressions.end())
            return {};

        return expression_it->second.front();
    }

    bool has(const IQueryTreeNode * node) const
    {
        return std::ranges::any_of(expressions, [node](const auto & expression) { return expression.get() == node; });
    }

    [[maybe_unused]] size_t size() const
    {
        return expressions.size();
    }

    bool empty() const
    {
        return expressions.empty();
    }

    void dump(WriteBuffer & buffer) const
    {
        buffer << "Expression resolve process stack size: " << expressions.size() << '\n';

        for (const auto & expression : expressions)
        {
            buffer << " Expression ";
            buffer << expression->formatASTForErrorMessage();

            const auto & alias = expression->getAlias();
            if (!alias.empty())
                buffer << " alias " << alias;

            buffer << '\n';
        }
    }

    [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }

private:
    static bool isAggregateOrGroupingFunction(const FunctionNode & function)
    {
        /// A window function is evaluated after aggregation: its arguments and window specification see GROUP BY keys
        /// already converted to Nullable. `isWindowFunction` tests only the window child, so it holds before resolution.
        /// The parser always lowercases the `grouping` function name (see `getFunctionLayer`
        /// in `ExpressionListParsers.cpp`), so the exact comparison is enough.
        return (AggregateFunctionFactory::instance().isAggregateFunctionName(function.getFunctionName())
                && !function.isWindowFunction())
            || function.getFunctionName() == "grouping";
    }

    QueryTreeNodes expressions;
    /// Whether the expression at the same position is counted in `aggregate_or_grouping_functions_counter`.
    std::vector<bool> expression_is_counted;
    size_t aggregate_or_grouping_functions_counter = 0;
    std::unordered_map<std::string, QueryTreeNodes> alias_name_to_expressions;
};

}
