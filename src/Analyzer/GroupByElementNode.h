#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** A `GROUP BY` element with an optional `WITH CLUSTER <distance>` modifier
  * (e.g. `GROUP BY event_time WITH CLUSTER 1800`).
  */
class GroupByElementNode;
using GroupByElementNodePtr = std::shared_ptr<GroupByElementNode>;

class GroupByElementNode final : public IQueryTreeNode
{
public:
    explicit GroupByElementNode(QueryTreeNodePtr expression_, bool with_cluster_ = false, Float64 cluster_distance_ = 0);

    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    bool withCluster() const
    {
        return with_cluster;
    }

    Float64 getClusterDistance() const
    {
        return cluster_distance;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::GROUP_BY_ELEMENT;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    static constexpr size_t expression_child_index = 0;
    static constexpr size_t children_size = 1;

    bool with_cluster = false;
    Float64 cluster_distance = 0;
};

}
