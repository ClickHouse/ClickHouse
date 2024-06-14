#pragma once

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

        if (const auto * function = node->as<FunctionNode>())
        {
            if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->getFunctionName()))
                ++aggregate_functions_counter;
        }

        expressions.emplace_back(node);
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

        if (const auto * function = top_expression->as<FunctionNode>())
        {
            if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->getFunctionName()))
                --aggregate_functions_counter;
        }

        expressions.pop_back();
    }

    [[maybe_unused]] const QueryTreeNodePtr & getRoot() const
    {
        return expressions.front();
    }

    const QueryTreeNodePtr & getTop() const
    {
        return expressions.back();
    }

    [[maybe_unused]] bool hasExpressionWithAlias(const std::string & alias) const
    {
        return alias_name_to_expressions.contains(alias);
    }

    bool hasAggregateFunction() const
    {
        return aggregate_functions_counter > 0;
    }

    QueryTreeNodePtr getExpressionWithAlias(const std::string & alias) const
    {
        auto expression_it = alias_name_to_expressions.find(alias);
        if (expression_it == alias_name_to_expressions.end())
            return {};

        return expression_it->second.front();
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
        buffer << expressions.size() << '\n';

        for (const auto & expression : expressions)
        {
            buffer << "Expression ";
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
    QueryTreeNodes expressions;
    size_t aggregate_functions_counter = 0;
    std::unordered_map<std::string, QueryTreeNodes> alias_name_to_expressions;
};

}
