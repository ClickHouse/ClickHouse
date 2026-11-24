#pragma once

#include <ranges>
#include <unordered_map>
#include <vector>
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
        bool is_aggregate_or_grouping = false;
        if (const auto * function = node->as<FunctionNode>())
            is_aggregate_or_grouping = isAggregateOrGroupingFunction(*function);

        entries.reserve(entries.size() + 1);

        if (node->hasAlias())
        {
            const auto & node_alias = node->getAlias();
            auto [it, inserted] = alias_name_to_expressions.try_emplace(node_alias);
            try
            {
                it->second.push_back(node);
            }
            catch (...)
            {
                if (inserted)
                    alias_name_to_expressions.erase(it);
                throw;
            }
        }

        if (is_aggregate_or_grouping)
            ++aggregate_or_grouping_functions_counter;

        entries.push_back(Entry{
            .node = node,
            .is_aggregate_or_grouping = is_aggregate_or_grouping,
        });
    }

    void pop() noexcept
    {
        const auto & top_entry = entries.back();
        const auto & top_expression = top_entry.node;
        const auto & top_expression_alias = top_expression->getAlias();

        if (!top_expression_alias.empty())
        {
            auto it = alias_name_to_expressions.find(top_expression_alias);
            if (it != alias_name_to_expressions.end())
            {
                auto & alias_expressions = it->second;
                alias_expressions.pop_back();

                if (alias_expressions.empty())
                    alias_name_to_expressions.erase(it);
            }
        }

        if (top_entry.is_aggregate_or_grouping)
            --aggregate_or_grouping_functions_counter;

        entries.pop_back();
    }

    [[nodiscard]] [[maybe_unused]] const QueryTreeNodePtr & getRoot() const
    {
        return entries.front().node;
    }

    [[nodiscard]] const QueryTreeNodePtr & getTop() const
    {
        return entries.back().node;
    }

    [[maybe_unused]] const QueryTreeNodePtr & operator[](int32_t n) const
    {
        if (n < 0)
            n = static_cast<int32_t>(entries.size()) + n - 1;
        return entries[n].node;
    }

    [[nodiscard]] [[maybe_unused]] bool hasExpressionWithAlias(const std::string & alias) const
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
    [[nodiscard]] bool hasAggregateOrGroupingFunction() const
    {
        return aggregate_or_grouping_functions_counter > 0;
    }

    [[nodiscard]] QueryTreeNodePtr getExpressionWithAlias(const std::string & alias) const
    {
        auto expression_it = alias_name_to_expressions.find(alias);
        if (expression_it == alias_name_to_expressions.end())
            return {};

        return expression_it->second.front();
    }

    bool has(const IQueryTreeNode * node) const
    {
        return std::ranges::any_of(entries, [node](const auto & entry) { return entry.node.get() == node; });
    }

    [[nodiscard]] [[maybe_unused]] size_t size() const
    {
        return entries.size();
    }

    [[nodiscard]] bool empty() const
    {
        return entries.empty();
    }

    void dump(WriteBuffer & buffer) const
    {
        buffer << "Expression resolve process stack size: " << entries.size() << '\n';

        for (const auto & entry : entries)
        {
            const auto & expression = entry.node;
            buffer << " Expression ";
            buffer << expression->formatASTForErrorMessage();

            const auto & alias = expression->getAlias();
            if (!alias.empty())
                buffer << " alias " << alias;

            buffer << '\n';
        }
    }

    [[nodiscard]] [[maybe_unused]] String dump() const
    {
        WriteBufferFromOwnString buffer;
        dump(buffer);

        return buffer.str();
    }

private:
    struct Entry
    {
        QueryTreeNodePtr node;
        bool is_aggregate_or_grouping = false;
    };

    static bool isAggregateOrGroupingFunction(const FunctionNode & function)
    {
        /// A window function is evaluated after aggregation: its arguments and window specification see GROUP BY keys
        /// already converted to Nullable. `isWindowCall` tests only the window child, so it holds before resolution.
        /// The parser always lowercases the `grouping` function name (see `getFunctionLayer`
        /// in `ExpressionListParsers.cpp`), so the exact comparison is enough.
        return (AggregateFunctionFactory::instance().isAggregateFunctionName(function.getFunctionName())
                && !function.isWindowCall())
            || function.getFunctionName() == "grouping";
    }

    std::vector<Entry> entries;
    size_t aggregate_or_grouping_functions_counter = 0;
    std::unordered_map<std::string, QueryTreeNodes> alias_name_to_expressions;
};

}
