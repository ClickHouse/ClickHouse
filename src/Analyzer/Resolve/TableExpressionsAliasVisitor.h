#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

class TableExpressionsAliasVisitor : public InDepthQueryTreeVisitor<TableExpressionsAliasVisitor>
{
public:
    explicit TableExpressionsAliasVisitor(IdentifierResolveScope & scope_)
        : scope(scope_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        updateAliasesIfNeeded(node, scope);
    }

    static bool needChildVisit(const QueryTreeNodePtr & node, const QueryTreeNodePtr & child)
    {
        auto node_type = node->getNodeType();

        switch (node_type)
        {
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                const auto & array_join_node = node->as<const ArrayJoinNode &>();
                return child.get() == array_join_node.getTableExpression().get();
            }
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = node->as<const JoinNode &>();
                return child.get() == join_node.getLeftTableExpression().get() || child.get() == join_node.getRightTableExpression().get();
            }
            default:
            {
                break;
            }
        }

        return false;
    }

    static void updateAliasesIfNeeded(const QueryTreeNodePtr & node, IdentifierResolveScope & scope)
    {
        if (!node->hasAlias())
            return;

        const auto & node_alias = node->getAlias();
        auto [it, inserted] = scope.aliases.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted && node != it->second)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Multiple table expressions with same alias {}. In scope {}",
                node_alias,
                scope.scope_node->formatASTForErrorMessage());
    }

private:
    IdentifierResolveScope & scope;
};

}
