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
    TableExpressionsAliasVisitor(IdentifierResolveScope & scope_, bool standard_mode_)
        : scope(scope_), standard_mode(standard_mode_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        updateAliasesIfNeeded(node);
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
            case QueryTreeNodeType::CROSS_JOIN:
            {
                /// All children of CROSS_JOIN are table expressions.
                return true;
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

private:
    void updateAliasesIfNeeded(const QueryTreeNodePtr & node)
    {
        if (!node->hasAlias())
            return;

        const auto & node_alias = node->getAlias();
        auto [_, inserted] = scope.aliases.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Multiple table expressions with same alias {}. In scope {}",
                node_alias,
                scope.scope_node->formatASTForErrorMessage());
        /// Keep the lowercase alias index in sync so case-insensitive table-alias resolution finds it.
        if (standard_mode)
            scope.aliases.registerAliasCaseInsensitive(node_alias, IdentifierLookupContext::TABLE_EXPRESSION);
    }

    IdentifierResolveScope & scope;
    bool standard_mode;
};

}
