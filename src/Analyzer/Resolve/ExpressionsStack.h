#pragma once

#include <ranges>
#include <IO/Operators.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

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

            /// Capture the decision here and replay it in pop(); it can't be recomputed there
            /// because resolving an IN rewrites its right-hand side in place (e.g. `x IN table`
            /// -> `x IN (subquery)`), which would unbalance subquery_function_depth.
            const bool is_subquery_scope = isSubqueryScopeFunction(function);
            subquery_function_scope_stack.push_back(is_subquery_scope);
            if (is_subquery_scope)
                ++subquery_function_depth;
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

            /// Replay the decision captured in push() rather than recomputing it; see push().
            if (subquery_function_scope_stack.back())
                --subquery_function_depth;
            subquery_function_scope_stack.pop_back();
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

    bool hasAggregateFunction() const
    {
        return aggregate_functions_counter > 0;
    }

    bool isInsideSubqueryFunction() const
    {
        return subquery_function_depth > 0;
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
    /// Whether this function opens a "subquery scope" — one whose resolution can mutate the
    /// query tree (IN->JOIN rewrite, unique-aliasing of subquery tables) in ways incompatible
    /// with reusing a shared cached expression. `exists` always does; an IN only when its
    /// right-hand side is a subquery or table, not a literal set (`x IN (1, 2, 3)`, `x IN 1`).
    static bool isSubqueryScopeFunction(const FunctionNode * function)
    {
        const auto & name = function->getFunctionName();

        if (name == "exists")
            return true;

        if (!isNameOfInFunction(name))
            return false;

        const auto & arguments = function->getArguments().getNodes();
        if (arguments.size() != 2)
            return true;

        return !isLiteralSetNode(arguments[1]);
    }

    /// Whether an IN right-hand side is a literal set rather than a subquery/table. Checked
    /// before the arguments are resolved, when a literal set is still a constant or a
    /// `tuple`/`array` function (a subquery or table is a query/identifier node, never matched).
    static bool isLiteralSetNode(const QueryTreeNodePtr & node)
    {
        if (!node)
            return false;

        if (node->getNodeType() == QueryTreeNodeType::CONSTANT)
            return true;

        if (const auto * function = node->as<FunctionNode>())
        {
            const auto & name = function->getFunctionName();
            return name == "tuple" || name == "array";
        }

        return false;
    }

    QueryTreeNodes expressions;
    size_t aggregate_functions_counter = 0;
    size_t subquery_function_depth = 0;
    /// Per-pushed-function record of isSubqueryScopeFunction, replayed in pop(); see push().
    std::vector<char> subquery_function_scope_stack;
    std::unordered_map<std::string, QueryTreeNodes> alias_name_to_expressions;
};

}
