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

    void dumpTreeWithPrefix(WriteBuffer & buffer, FormatState & format_state, size_t indent, std::string_view kind) const;

    void dumpTreeIfNotEmpty(WriteBuffer & buffer, FormatState & format_state, size_t indent, std::string_view kind) const
    {
        if (children.empty())
            return;
        dumpTreeWithPrefix(buffer, format_state, indent, kind);
    }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState &) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl() const override;
};

}
