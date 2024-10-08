#pragma once

#include <Core/Joins.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Join node represents join in query tree.
  *
  * For JOIN without join expression, JOIN expression is null.
  * Example: SELECT id FROM test_table_1 AS t1, test_table_2 AS t2;
  *
  * For JOIN with USING, JOIN expression contains list of identifier nodes. These nodes must be resolved
  * during query analysis pass.
  * Example: SELECT id FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 USING (id);
  *
  * For JOIN with ON, JOIN expression contains single expression.
  * Example: SELECT id FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 ON t1.id = t2.id;
  */
class JoinNode;
using JoinNodePtr = std::shared_ptr<JoinNode>;

class JoinNode final : public IQueryTreeNode
{
public:
    /** Construct join node with left table expression, right table expression and join expression.
      * Example: SELECT id FROM test_table_1 INNER JOIN test_table_2 ON expression.
      *
      * test_table_1 - left table expression.
      * test_table_2 - right table expression.
      * expression - join expression.
      */
    JoinNode(QueryTreeNodePtr left_table_expression_,
        QueryTreeNodePtr right_table_expression_,
        QueryTreeNodePtr join_expression_,
        JoinLocality locality_,
        JoinStrictness strictness_,
        JoinKind kind_,
        bool is_using_join_expression_);

    /// Get left table expression
    const QueryTreeNodePtr & getLeftTableExpression() const
    {
        return children[left_table_expression_child_index];
    }

    /// Get left table expression
    QueryTreeNodePtr & getLeftTableExpression()
    {
        return children[left_table_expression_child_index];
    }

    /// Get right table expression
    const QueryTreeNodePtr & getRightTableExpression() const
    {
        return children[right_table_expression_child_index];
    }

    /// Get right table expression
    QueryTreeNodePtr & getRightTableExpression()
    {
        return children[right_table_expression_child_index];
    }

    /// Returns true if join has join expression, false otherwise
    bool hasJoinExpression() const
    {
        return children[join_expression_child_index] != nullptr;
    }

    /// Get join expression
    const QueryTreeNodePtr & getJoinExpression() const
    {
        return children[join_expression_child_index];
    }

    /// Get join expression
    QueryTreeNodePtr & getJoinExpression()
    {
        return children[join_expression_child_index];
    }

    /// Returns true if join has USING join expression, false otherwise
    bool isUsingJoinExpression() const
    {
        return hasJoinExpression() && is_using_join_expression;
    }

    /// Returns true if join has ON join expression, false otherwise
    bool isOnJoinExpression() const
    {
        return hasJoinExpression() && !is_using_join_expression;
    }

    /// Get join locality
    JoinLocality getLocality() const
    {
        return locality;
    }

    /// Set join locality
    void setLocality(JoinLocality locality_value)
    {
        locality = locality_value;
    }

    /// Get join strictness
    JoinStrictness getStrictness() const
    {
        return strictness;
    }

    /// Get join kind
    JoinKind getKind() const
    {
        return kind;
    }

    /// Convert join node to ASTTableJoin
    ASTPtr toASTTableJoin() const;

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::JOIN;
    }

    /*
     * Convert CROSS to INNER JOIN - changes JOIN kind and sets a new join expression
     * (that was moved from WHERE clause).
     * Expects the current kind to be CROSS (and join expression to be null because of that).
     */
    void crossToInner(const QueryTreeNodePtr & join_expression_);

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    JoinLocality locality = JoinLocality::Unspecified;
    JoinStrictness strictness = JoinStrictness::Unspecified;
    JoinKind kind = JoinKind::Inner;
    bool is_using_join_expression;

    static constexpr size_t left_table_expression_child_index = 0;
    static constexpr size_t right_table_expression_child_index = 1;
    static constexpr size_t join_expression_child_index = 2;
    static constexpr size_t children_size = join_expression_child_index + 1;
};

}

