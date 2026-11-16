#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

#include <Interpreters/WindowDescription.h>

namespace DB
{

/** Window node represents window function window description.
  *
  * Example: SELECT * FROM test_table WINDOW window AS (PARTITION BY id);
  * window AS (PARTITION BY id) - window node.
  *
  * Example: SELECT count() OVER (PARTITION BY id) FROM test_table;
  * PARTITION BY id - window node.
  *
  * Window node can also refer to its parent window node.
  * Example: SELECT count() OVER (parent_window ORDER BY id) FROM test_table WINDOW parent_window AS (PARTITION BY id);
  * parent_window ORDER BY id - window node.
  *
  * Window node initially initialized with window frame.
  *
  * If window frame has OFFSET begin type, additionally frame begin offset node must be initialized.
  * If window frame has OFFSET end type, additionally frame end offset node must be initialized.
  * During query analysis pass they must be resolved, validated and window node window frame offset constants must be updated.
  */
class WindowNode;
using WindowNodePtr = std::shared_ptr<WindowNode>;

class WindowNode final : public IQueryTreeNode
{
public:
    /// Initialize window node with window frame
    explicit WindowNode(WindowFrame window_frame_);

    /// Get window node window frame
    const WindowFrame & getWindowFrame() const
    {
        return window_frame;
    }

    /// Get window node window frame
    WindowFrame & getWindowFrame()
    {
        return window_frame;
    }

    /// Returns true if window node has parent window name, false otherwise
    bool hasParentWindowName() const
    {
        return parent_window_name.empty();
    }

    /// Get parent window name
    const String & getParentWindowName() const
    {
        return parent_window_name;
    }

    /// Set parent window name
    void setParentWindowName(String parent_window_name_value)
    {
        parent_window_name = std::move(parent_window_name_value);
    }

    /// Returns true if window node has order by, false otherwise
    bool hasOrderBy() const
    {
        return !getOrderBy().getNodes().empty();
    }

    /// Get order by
    const ListNode & getOrderBy() const
    {
        return children[order_by_child_index]->as<const ListNode &>();
    }

    /// Get order by
    ListNode & getOrderBy()
    {
        return children[order_by_child_index]->as<ListNode &>();
    }

    /// Get order by node
    const QueryTreeNodePtr & getOrderByNode() const
    {
        return children[order_by_child_index];
    }

    /// Get order by node
    QueryTreeNodePtr & getOrderByNode()
    {
        return children[order_by_child_index];
    }

    /// Returns true if window node has partition by, false otherwise
    bool hasPartitionBy() const
    {
        return !getPartitionBy().getNodes().empty();
    }

    /// Get partition by
    const ListNode & getPartitionBy() const
    {
        return children[partition_by_child_index]->as<const ListNode &>();
    }

    /// Get partition by
    ListNode & getPartitionBy()
    {
        return children[partition_by_child_index]->as<ListNode &>();
    }

    /// Get partition by node
    const QueryTreeNodePtr & getPartitionByNode() const
    {
        return children[partition_by_child_index];
    }

    /// Get partition by node
    QueryTreeNodePtr & getPartitionByNode()
    {
        return children[partition_by_child_index];
    }

    /// Returns true if window node has FRAME begin offset, false otherwise
    bool hasFrameBeginOffset() const
    {
        return getFrameBeginOffsetNode() != nullptr;
    }

    /// Get FRAME begin offset node
    const QueryTreeNodePtr & getFrameBeginOffsetNode() const
    {
        return children[frame_begin_offset_child_index];
    }

    /// Get FRAME begin offset node
    QueryTreeNodePtr & getFrameBeginOffsetNode()
    {
        return children[frame_begin_offset_child_index];
    }

    /// Returns true if window node has FRAME end offset, false otherwise
    bool hasFrameEndOffset() const
    {
        return getFrameEndOffsetNode() != nullptr;
    }

    /// Get FRAME end offset node
    const QueryTreeNodePtr & getFrameEndOffsetNode() const
    {
        return children[frame_end_offset_child_index];
    }

    /// Get FRAME end offset node
    QueryTreeNodePtr & getFrameEndOffsetNode()
    {
        return children[frame_end_offset_child_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::WINDOW;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    static constexpr size_t order_by_child_index = 0;
    static constexpr size_t partition_by_child_index = 1;
    static constexpr size_t frame_begin_offset_child_index = 3;
    static constexpr size_t frame_end_offset_child_index = 4;
    static constexpr size_t children_size = frame_end_offset_child_index + 1;

    WindowFrame window_frame;
    String parent_window_name;
};

}
