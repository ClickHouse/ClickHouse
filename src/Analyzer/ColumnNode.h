#pragma once

#include <Core/NamesAndTypes.h>

#include <Analyzer/IQueryTreeNode.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Column node represents column in query tree.
  * Column node can have weak pointer to its column source.
  * Column source can be table expression, lambda, subquery.
  *
  * For table ALIAS columns. Column node must contain expression.
  * For ARRAY JOIN join expression column. Column node must contain expression.
  *
  * During query analysis pass identifier node is resolved into column. See IdentifierNode.h.
  *
  * Examples:
  * SELECT id FROM test_table. id is identifier that must be resolved to column node during query analysis pass.
  * SELECT lambda(x -> x + 1, [1,2,3]). x is identifier inside lambda that must be resolved to column node during query analysis pass.
  *
  * Column node is initialized with column name, type and column source weak pointer.
  * In case of ALIAS column node is initialized with column name, type, alias expression and column source weak pointer.
  */
class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;

class ColumnNode final : public IQueryTreeNode
{
public:
    /// Construct column node with column name, type, column expression and column source weak pointer
    ColumnNode(NameAndTypePair column_, QueryTreeNodePtr expression_node_, ColumnSourceNodeWeakPtr column_source_);

    /// Construct column node with column name, type and column source weak pointer
    ColumnNode(NameAndTypePair column_, ColumnSourceNodeWeakPtr column_source_);

    /** Construct column node with an explicit column source id snapshot.
      * Used for cloning: copies the id even if the source has expired.
      */
    ColumnNode(NameAndTypePair column_, ColumnSourceNodeWeakPtr column_source_, ColumnSourceId column_source_id_);

    /// Defined out of line so the forward-declared IColumnSourceNode in the weak pointer member
    /// does not need to be complete at every include site.
    ~ColumnNode() override;

    /// Get column
    const NameAndTypePair & getColumn() const
    {
        return column;
    }

    /// Get column name
    const String & getColumnName() const
    {
        return column.name;
    }

    /// Get column type
    const DataTypePtr & getColumnType() const
    {
        return column.type;
    }

    /// Set column type
    void setColumnType(DataTypePtr column_type)
    {
        column.type = std::move(column_type);
    }

    /// Returns true if column node has expression, false otherwise
    bool hasExpression() const
    {
        return children[expression_child_index] != nullptr;
    }

    /// Get column node expression node
    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    /// Get column node expression node
    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    /// Get column node expression node, if there are no expression node exception is thrown
    QueryTreeNodePtr & getExpressionOrThrow()
    {
        if (!children[expression_child_index])
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column expression is not initialized");

        return children[expression_child_index];
    }

    /// Set column node expression node
    void setExpression(QueryTreeNodePtr expression_value)
    {
        children[expression_child_index] = std::move(expression_value);
    }

    /** Get column source.
      * If column source is not valid logical exception is thrown.
      */
    ColumnSourceNodePtr getColumnSource() const;

    /** Get column source.
      * If column source is not valid null is returned.
      */
    ColumnSourceNodePtr getColumnSourceOrNull() const;

    /** Get the id of the column source instance this column refers to.
      * Returns INVALID_COLUMN_SOURCE_ID if the column has no source.
      * The id stays valid even after the source node itself is destroyed.
      */
    ColumnSourceId getColumnSourceId() const
    {
        return column_source_id;
    }

    void setColumnSource(const ColumnSourceNodePtr & source);

    /** Re-point the column source to its clone after the column node was cloned, and refresh the
      * column source id snapshot. If the column source is not in the map (not part of the cloned
      * subtree or the replacement map), the column keeps referencing the previous source and it is
      * expected. Used by IQueryTreeNode::cloneAndReplace.
      */
    void remapColumnSourceAfterClone(const std::unordered_map<const IQueryTreeNode *, QueryTreeNodePtr> & old_pointer_to_new_pointer);

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::COLUMN;
    }

    DataTypePtr getResultType() const override
    {
        return column.type;
    }

    void convertToNullable() override
    {
        column.type = makeNullableOrLowCardinalityNullableSafe(column.type);
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    NameAndTypePair column;

    ColumnSourceNodeWeakPtr column_source;

    /// Snapshot of the source's column source id, kept in sync with the column source weak pointer.
    ColumnSourceId column_source_id = INVALID_COLUMN_SOURCE_ID;

    static constexpr size_t expression_child_index = 0;
    static constexpr size_t children_size = expression_child_index + 1;
};

}
