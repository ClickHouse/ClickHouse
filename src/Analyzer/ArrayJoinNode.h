#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>

namespace DB
{

/** Array join node represents array join in query tree.
  *
  * In query tree join expression is represented by list query tree node.
  *
  * Example: SELECT id FROM test_table ARRAY JOIN [1, 2, 3] as a.
  * Example: SELECT id FROM test_table ARRAY JOIN [1, 2, 3] as a, [4, 5, 6] as b.
  */
class ArrayJoinNode;
using ArrayJoinNodePtr = std::shared_ptr<ArrayJoinNode>;

class ArrayJoinNode final : public IQueryTreeNode
{
public:
    /** Construct array join node with table expression.
      * Example: SELECT id FROM test_table ARRAY JOIN [1, 2, 3] as a.
      * test_table - table expression.
      * join_expression_list - list of array join expressions.
      */
    ArrayJoinNode(QueryTreeNodePtr table_expression_, QueryTreeNodePtr join_expressions_, bool is_left_);

    /// Get table expression
    const QueryTreeNodePtr & getTableExpression() const
    {
        return children[table_expression_child_index];
    }

    /// Get table expression
    QueryTreeNodePtr & getTableExpression()
    {
        return children[table_expression_child_index];
    }

    /// Get join expressions
    const ListNode & getJoinExpressions() const
    {
        return children[join_expressions_child_index]->as<const ListNode &>();
    }

    /// Get join expressions
    ListNode & getJoinExpressions()
    {
        return children[join_expressions_child_index]->as<ListNode &>();
    }

    /// Get join expressions node
    const QueryTreeNodePtr & getJoinExpressionsNode() const
    {
        return children[join_expressions_child_index];
    }

    QueryTreeNodePtr & getJoinExpressionsNode()
    {
        return children[join_expressions_child_index];
    }

    bool isLeft() const
    {
        return is_left;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::ARRAY_JOIN;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    explicit ArrayJoinNode(bool is_left_)
        : is_left(is_left_)
    {}

    bool is_left = false;

    static constexpr size_t table_expression_child_index = 0;
    static constexpr size_t join_expressions_child_index = 1;
    static constexpr size_t children_size = join_expressions_child_index + 1;
};

}

