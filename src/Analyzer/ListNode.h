#pragma once

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** List node represents list of query tree nodes in query tree.
  *
  * Example: SELECT column_1, 1, 'constant_value' FROM table.
  * column_1, 1, 'constant_value' is list query tree node.
  */
class ListNode;
using ListNodePtr = std::shared_ptr<ListNode>;

class ListNode final : public IQueryTreeNode
{
public:
    /// Get list nodes
    const QueryTreeNodes & getNodes() const
    {
        return children;
    }

    /// Get list nodes
    QueryTreeNodes & getNodes()
    {
        return children;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::LIST;
    }

    void dumpTree(WriteBuffer & buffer, size_t indent) const override;

    String getName() const override;

protected:
    void updateTreeHashImpl(HashState &) const override;

    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;
};

}
