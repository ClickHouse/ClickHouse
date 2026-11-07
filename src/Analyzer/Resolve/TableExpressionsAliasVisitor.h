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
                return child.get() == array_join_node.getTableExpressionNode().get();
            }
            case QueryTreeNodeType::CROSS_JOIN:
            {
                /// All children of CROSS_JOIN are table expressions.
                return true;
            }
            case QueryTreeNodeType::JOIN:
            {
                const auto & join_node = node->as<const JoinNode &>();
                return child.get() == join_node.getLeftTableExpressionNode().get() || child.get() == join_node.getRightTableExpressionNode().get();
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
        /// The alias of `unnest(...)` qualifies only its columns, so it is not a table expression alias,
        /// but it must not repeat one.
        if (const auto * array_join_node = node->as<ArrayJoinNode>(); array_join_node && array_join_node->isUnnest())
        {
            const auto & unnest_alias = array_join_node->getUnnestAlias();
            if (scope.aliases.alias_name_to_table_expression_node.contains(unnest_alias) || !unnest_aliases.insert(unnest_alias).second)
                throwMultipleTableExpressionsWithSameAlias(unnest_alias);
        }

        if (!node->hasAlias())
            return;

        const auto & node_alias = node->getAlias();
        auto [_, inserted] = scope.aliases.alias_name_to_table_expression_node.emplace(node_alias, node);
        if (!inserted || unnest_aliases.contains(node_alias))
            throwMultipleTableExpressionsWithSameAlias(node_alias);
    }

    [[noreturn]] void throwMultipleTableExpressionsWithSameAlias(const String & alias) const
    {
        throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
            "Multiple table expressions with same alias {}. In scope {}",
            alias,
            scope.scope_node->formatASTForErrorMessage());
    }

    IdentifierResolveScope & scope;
    std::unordered_set<String> unnest_aliases;
};

}
