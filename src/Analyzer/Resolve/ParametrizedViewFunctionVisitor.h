#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/formatAST.h>

namespace DB
{

class ParametrizedViewFunctionParamsVisitor : public InDepthQueryTreeVisitor<ParametrizedViewFunctionParamsVisitor>
{
public:
    ParametrizedViewFunctionParamsVisitor(
        std::function<void(QueryTreeNodePtr)> resolve_node_,
        const ContextPtr & context_)
        : context(context_)
        , resolve_node(resolve_node_)
    {
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto * function_node = node->as<FunctionNode>())
        {
            if (function_node->getFunctionName() != "equals")
                return;

            auto nodes = function_node->getArguments().getNodes();
            if (nodes.size() != 2)
                return;

            if (auto * identifier_node = nodes[0]->as<IdentifierNode>())
            {
                resolve_node(nodes[1]);
                auto resolved_value = evaluateConstantExpressionOrIdentifierAsLiteral(nodes[1]->toAST(), context);
                auto resolved_value_str = convertFieldToString(resolved_value->as<ASTLiteral>()->value);
                params[identifier_node->getIdentifier().getFullName()] = resolved_value_str;
            }
        }
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &) { return true; }

    NameToNameMap getParams() const { return params; }

private:
    NameToNameMap params;
    const ContextPtr context;
    std::function<void(QueryTreeNodePtr)> resolve_node;
};
}
