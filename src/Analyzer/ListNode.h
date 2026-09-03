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
    using iterator = QueryTreeNodes::iterator;
    using const_iterator = QueryTreeNodes::const_iterator;

    /// Initialize list node with empty nodes
    ListNode();

    /// Initialize list node with nodes
    explicit ListNode(QueryTreeNodes nodes);

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

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    iterator begin() { return children.begin(); }
    const_iterator begin() const { return children.begin(); }

    iterator end() { return children.end(); }
    const_iterator end() const { return children.end(); }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState &, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;
};

}
