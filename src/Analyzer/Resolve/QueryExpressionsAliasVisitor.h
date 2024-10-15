#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Resolve/ScopeAliases.h>
#include <Analyzer/LambdaNode.h>

namespace DB
{

/** Visitor that extracts expression and function aliases from node and initialize scope tables with it.
  * Does not go into child lambdas and queries.
  *
  * Important:
  * Identifier nodes with aliases are added both in alias to expression and alias to function map.
  *
  * These is necessary because identifier with alias can give alias name to any query tree node.
  *
  * Example:
  * WITH (x -> x + 1) AS id, id AS value SELECT value(1);
  * In this example id as value is identifier node that has alias, during scope initialization we cannot derive
  * that id is actually lambda or expression.
  *
  * There are no easy solution here, without trying to make full featured expression resolution at this stage.
  * Example:
  * WITH (x -> x + 1) AS id, id AS id_1, id_1 AS id_2 SELECT id_2(1);
  * Example: SELECT a, b AS a, b AS c, 1 AS c;
  *
  * It is client responsibility after resolving identifier node with alias, make following actions:
  * 1. If identifier node was resolved in function scope, remove alias from scope expression map.
  * 2. If identifier node was resolved in expression scope, remove alias from scope function map.
  *
  * That way we separate alias map initialization and expressions resolution.
  */
class QueryExpressionsAliasVisitor : public InDepthQueryTreeVisitor<QueryExpressionsAliasVisitor>
{
public:
    explicit QueryExpressionsAliasVisitor(ScopeAliases & aliases_)
        : aliases(aliases_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
        updateAliasesIfNeeded(node, false /*is_lambda_node*/);
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        if (auto * /*lambda_node*/ _ = child->as<LambdaNode>())
        {
            updateAliasesIfNeeded(child, true /*is_lambda_node*/);
            return false;
        }
        if (auto * query_tree_node = child->as<QueryNode>())
        {
            if (query_tree_node->isCTE())
                return false;

            updateAliasesIfNeeded(child, false /*is_lambda_node*/);
            return false;
        }
        if (auto * union_node = child->as<UnionNode>())
        {
            if (union_node->isCTE())
                return false;

            updateAliasesIfNeeded(child, false /*is_lambda_node*/);
            return false;
        }

        return true;
    }
private:
    void addDuplicatingAlias(const QueryTreeNodePtr & node)
    {
        aliases.nodes_with_duplicated_aliases.emplace(node);
        auto cloned_node = node->clone();
        aliases.cloned_nodes_with_duplicated_aliases.emplace_back(cloned_node);
        aliases.nodes_with_duplicated_aliases.emplace(cloned_node);
    }

    void updateAliasesIfNeeded(const QueryTreeNodePtr & node, bool is_lambda_node)
    {
        if (!node->hasAlias())
            return;

        // We should not resolve expressions to WindowNode
        if (node->getNodeType() == QueryTreeNodeType::WINDOW)
            return;

        const auto & alias = node->getAlias();

        if (is_lambda_node)
        {
            if (aliases.alias_name_to_expression_node->contains(alias))
                addDuplicatingAlias(node);

            auto [_, inserted] = aliases.alias_name_to_lambda_node.insert(std::make_pair(alias, node));
            if (!inserted)
             addDuplicatingAlias(node);

            return;
        }

        if (aliases.alias_name_to_lambda_node.contains(alias))
            addDuplicatingAlias(node);

        auto [_, inserted] = aliases.alias_name_to_expression_node->insert(std::make_pair(alias, node));
        if (!inserted)
            addDuplicatingAlias(node);

        /// If node is identifier put it into transitive aliases map.
        if (const auto * identifier = typeid_cast<const IdentifierNode *>(node.get()))
            aliases.transitive_aliases.insert(std::make_pair(alias, identifier->getIdentifier()));
    }

    ScopeAliases & aliases;
};

}
