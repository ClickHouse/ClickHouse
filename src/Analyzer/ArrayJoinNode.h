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
  * In query tree array join expressions are represented by list query tree node.
  *
  * Example: SELECT id FROM test_table ARRAY JOIN [1, 2, 3] as a.
  *
  * Multiple expressions can be inside single array join.
  * Example: SELECT id FROM test_table ARRAY JOIN [1, 2, 3] as a, [4, 5, 6] as b.
  * Example: SELECT id FROM test_table ARRAY JOIN array_column_1 AS value_1, array_column_2 AS value_2.
  *
  * Multiple array joins can be inside JOIN TREE.
  * Example: SELECT id FROM test_table ARRAY JOIN array_column_1 ARRAY JOIN array_column_2.
  *
  * Array join can be used inside JOIN TREE with ordinary JOINS.
  * Example: SELECT t1.id FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id ARRAY JOIN [1,2,3];
  * Example: SELECT t1.id FROM test_table_1 AS t1 ARRAY JOIN [1,2,3] INNER JOIN test_table_2 AS t2 ON t1.id = t2.id;
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

    /// Get join expressions node
    QueryTreeNodePtr & getJoinExpressionsNode()
    {
        return children[join_expressions_child_index];
    }

    /// Returns true if array join is left, false otherwise
    bool isLeft() const
    {
        return is_left;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::ARRAY_JOIN;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    bool is_left = false;

    static constexpr size_t table_expression_child_index = 0;
    static constexpr size_t join_expressions_child_index = 1;
    static constexpr size_t children_size = join_expressions_child_index + 1;
};

}

