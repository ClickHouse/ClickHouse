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
        updateAliasesIfNeeded(node);
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        if (auto * /*lambda_node*/ _ = child->as<LambdaNode>())
        {
            updateAliasesIfNeeded(child);
            return false;
        }
        if (auto * query_tree_node = child->as<QueryNode>())
        {
            if (query_tree_node->isCTE())
                return false;

            updateAliasesIfNeeded(child);
            return false;
        }
        if (auto * union_node = child->as<UnionNode>())
        {
            if (union_node->isCTE())
                return false;

            updateAliasesIfNeeded(child);
            return false;
        }

        return true;
    }
private:
    void addDuplicatingAlias(const QueryTreeNodePtr & node)
    {
        aliases.nodes_with_duplicated_aliases.emplace_back(node);
    }

    void updateAliasesIfNeeded(const QueryTreeNodePtr & node)
    {
        if (!node->hasAlias())
            return;

        auto node_type = node->getNodeType();

        // We should not resolve expressions to WindowNode
        if (node_type == QueryTreeNodeType::WINDOW)
            return;

        const auto & alias = node->getAlias();
        auto cloned_alias_node = node->clone();

        switch (node_type)
        {
        case QueryTreeNodeType::LAMBDA:
            {
                auto [_, inserted] = aliases.alias_name_to_lambda_node.emplace(alias, cloned_alias_node);
                if (!inserted || aliases.alias_name_to_expression_node.contains(alias))
                    addDuplicatingAlias(cloned_alias_node);
                break;
            }
        case QueryTreeNodeType::IDENTIFIER:
            {
                auto [_1, inserted_expression] = aliases.alias_name_to_expression_node.emplace(alias, cloned_alias_node);
                bool inserted_lambda           = true; // Avoid adding to duplicating aliases if identifier is compound.
                bool inserted_table_expression = true; // Avoid adding to duplicating aliases if identifier is compound.

                // Alias to compound identifier cannot reference table expression or lambda.
                // Example: SELECT x.b as x FROM (SELECT 1 as a, 2 as b) as x
                auto * identifier_node = node->as<IdentifierNode>();
                if (identifier_node->getIdentifier().isShort())
                {
                    inserted_lambda = aliases.alias_name_to_lambda_node.emplace(alias, cloned_alias_node).second;
                    inserted_table_expression = aliases.alias_name_to_table_expression_node.emplace(alias, cloned_alias_node).second;
                }

                if (!inserted_expression || !inserted_lambda || !inserted_table_expression)
                    addDuplicatingAlias(cloned_alias_node);
                break;
            }
        default:
            {
                auto [_, inserted] = aliases.alias_name_to_expression_node.emplace(alias, cloned_alias_node);
                if (!inserted || aliases.alias_name_to_lambda_node.contains(alias))
                    addDuplicatingAlias(cloned_alias_node);
                break;
            }
        }
    }

    ScopeAliases & aliases;
};

}
