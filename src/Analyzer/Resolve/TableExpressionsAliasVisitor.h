#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/Resolve/StandardNameMatching.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
}

class TableExpressionsAliasVisitor : public InDepthQueryTreeVisitor<TableExpressionsAliasVisitor>
{
public:
    TableExpressionsAliasVisitor(IdentifierResolveScope & scope_, NameMatchMode name_match_mode_)
        : scope(scope_)
        , name_match_mode(name_match_mode_)
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
        throwIfCaseSiblingTableAlias(node);
        auto [_, inserted] = scope.aliases.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Multiple table expressions with same alias {}. In scope {}",
                node_alias,
                scope.scope_node->formatASTForErrorMessage());
    }

    /// `standard` matching: unquoted table expression aliases that differ only in character case
    /// are rejected in one scope, mirroring the alias registration contract.
    void throwIfCaseSiblingTableAlias(const QueryTreeNodePtr & node)
    {
        if (name_match_mode != NameMatchMode::Standard || node->getAliasQuote() == IdentifierPartQuote::DoubleQuoted)
            return;

        const auto * sibling = findCaseSiblingName(scope.aliases.alias_name_to_table_expression_node, node->getAlias(),
            [](const String &, const QueryTreeNodePtr & existing) { return existing->getAliasQuote() == IdentifierPartQuote::DoubleQuoted; });
        if (sibling)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Table expression alias {} cannot be registered: its name differs only in character case from alias {} in the same scope. "
                "Double-quote the alias names to distinguish them",
                backQuoteIfNeed(node->getAlias()), backQuoteIfNeed(*sibling));
    }

    IdentifierResolveScope & scope;
    NameMatchMode name_match_mode;
};

}
