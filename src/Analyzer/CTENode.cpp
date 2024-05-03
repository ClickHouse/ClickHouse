
#include <Analyzer/CTENode.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Storages/IStorage.h>
#include "Analyzer/QueryNode.h"
#include "Analyzer/UnionNode.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSubquery.h"

namespace DB
{
CTENode::CTENode(QueryTreeNodePtr query_or_union_node) : IQueryTreeNode(1)
{
    children[0] = std::move(query_or_union_node);
    query_tree_node = children[0];
    union_node = query_tree_node->as<const UnionNode>();
    query_node = query_tree_node->as<const QueryNode>();

    if (!union_node && !query_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CTENode can be created only from UnionNode or QueryNode, but get: {}", query_tree_node->getNodeTypeName());

}

void CTENode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CTE SCAN id: " << format_state.getNodeId(this);

    if (is_materialized_cte)
        buffer << ", is_materialized_cte: " << is_materialized_cte;

    if (materialized_cte_engine)
        buffer << ", materialized_cte_engine: " << materialized_cte_engine->formatForLogging();

    if (!cte_name.empty())
        buffer << ", cte_name: " << cte_name;

    if (!unique_cte_name.empty())
        buffer << ", unique_cte_name: " << unique_cte_name;

    if (cte_ref.use_count() > 0)
        buffer << ", cte_ref: " << cte_ref.use_count();

    if (future_table)
        buffer << ", materialized_table: " << future_table->external_table->getStorageID().getFullNameNotQuoted() << " ENGINE = " << future_table->external_table->getName();
}

bool CTENode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const CTENode &>(rhs);
    if ((materialized_cte_engine && !rhs_typed.materialized_cte_engine) || (!materialized_cte_engine && rhs_typed.materialized_cte_engine))
        return false;
    return is_materialized_cte == rhs_typed.is_materialized_cte && cte_name == rhs_typed.cte_name && unique_cte_name == rhs_typed.unique_cte_name
        && (!materialized_cte_engine || materialized_cte_engine->getTreeHash(true) == rhs_typed.materialized_cte_engine->getTreeHash(true));
}

void CTENode::updateTreeHashImpl(HashState & hash_state, CompareOptions) const
{
    hash_state.update(is_materialized_cte);
    hash_state.update(cte_name.size());
    hash_state.update(cte_name);
    hash_state.update(cte_name.size());
    hash_state.update(unique_cte_name);
    if (materialized_cte_engine)
        materialized_cte_engine->updateTreeHashImpl(hash_state, true);
}

QueryTreeNodePtr CTENode::cloneImpl() const
{
    auto cloned_node = std::make_shared<CTENode>(getQueryNode()->clone());
    cloned_node->is_materialized_cte = is_materialized_cte;
    cloned_node->cte_name = cte_name;
    cloned_node->unique_cte_name = unique_cte_name;
    if (materialized_cte_engine)
        cloned_node->materialized_cte_engine = materialized_cte_engine->clone();
    cloned_node->cte_ref = cte_ref;
    return cloned_node;
}

ASTPtr CTENode::toASTImpl(const ConvertToASTOptions &) const
{
    if (future_table)
    {
        auto res = std::make_shared<ASTIdentifier>(future_table->external_table->getStorageID().getFullNameNotQuoted());
        res->setAlias(future_table->name);
        return res;
    }
    auto query = query_tree_node->toAST();
    bool set_subquery_cte_name = !(union_node && union_node->hasRecursiveCTETable());
    if (auto * sub_query = query->as<ASTSubquery>(); sub_query && set_subquery_cte_name)
        sub_query->cte_name = cte_name;
    return query;
}

NamesAndTypes CTENode::getProjectionColumns() const
{
    if (query_node)
        return query_node->getProjectionColumns();
    else
        return union_node->computeProjectionColumns();
}

ContextMutablePtr CTENode::getMutableContext() const
{
    return query_node ? query_node->getMutableContext() : union_node->getMutableContext();
}

}
