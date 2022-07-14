#pragma once

#include <Analyzer/Identifier.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Query node represents query in query tree.
  * TODO: CTE.
  */
class QueryNode;
using QueryNodePtr = std::shared_ptr<QueryNode>;

class QueryNode final : public IQueryTreeNode
{
public:
    QueryNode();

    const ListNode & getWith() const
    {
        return children[with_child_index]->as<const ListNode &>();
    }

    ListNode & getWith()
    {
        return children[with_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getWithNode() const
    {
        return children[with_child_index];
    }

    QueryTreeNodePtr & getWithNode()
    {
        return children[with_child_index];
    }

    const ListNode & getProjection() const
    {
        return children[projection_child_index]->as<const ListNode &>();
    }

    ListNode & getProjection()
    {
        return children[projection_child_index]->as<ListNode &>();
    }

    const QueryTreeNodePtr & getProjectionNode() const
    {
        return children[projection_child_index];
    }

    QueryTreeNodePtr & getProjectionNode()
    {
        return children[projection_child_index];
    }

    const QueryTreeNodePtr & getFrom() const
    {
        return children[from_child_index];
    }

    QueryTreeNodePtr & getFrom()
    {
        return children[from_child_index];
    }

    const QueryTreeNodePtr & getPrewhere() const
    {
        return children[prewhere_child_index];
    }

    QueryTreeNodePtr & getPrewhere()
    {
        return children[prewhere_child_index];
    }

    const QueryTreeNodePtr & getWhere() const
    {
        return children[where_child_index];
    }

    QueryTreeNodePtr & getWhere()
    {
        return children[where_child_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::QUERY;
    }

    void dumpTree(WriteBuffer & buffer, size_t indent) const override;

protected:
    void updateTreeHashImpl(HashState &) const override;

    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    static constexpr size_t with_child_index = 0;
    static constexpr size_t projection_child_index = 1;
    static constexpr size_t from_child_index = 2;
    static constexpr size_t prewhere_child_index = 3;
    static constexpr size_t where_child_index = 4;
    static constexpr size_t group_by_child_index = 5;
    static constexpr size_t having_child_index = 6;
    static constexpr size_t order_by_child_index = 7;
    static constexpr size_t limit_child_index = 8;
    static constexpr size_t children_size = where_child_index + 1;
};

}
