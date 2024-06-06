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
    ColumnNode(NameAndTypePair column_, QueryTreeNodePtr expression_node_, QueryTreeNodeWeakPtr column_source_);

    /// Construct column node with column name, type and column source weak pointer
    ColumnNode(NameAndTypePair column_, QueryTreeNodeWeakPtr column_source_);

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
    QueryTreeNodePtr getColumnSource() const;

    /** Get column source.
      * If column source is not valid null is returned.
      */
    QueryTreeNodePtr getColumnSourceOrNull() const;

    void setColumnSource(const QueryTreeNodePtr & source)
    {
        getSourceWeakPointer() = source;
    }

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
        column.type = makeNullableSafe(column.type);
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    const QueryTreeNodeWeakPtr & getSourceWeakPointer() const
    {
        return weak_pointers[source_weak_pointer_index];
    }

    QueryTreeNodeWeakPtr & getSourceWeakPointer()
    {
        return weak_pointers[source_weak_pointer_index];
    }

    NameAndTypePair column;

    static constexpr size_t expression_child_index = 0;
    static constexpr size_t children_size = expression_child_index + 1;

    static constexpr size_t source_weak_pointer_index = 0;
    static constexpr size_t weak_pointers_size = source_weak_pointer_index + 1;
};

}
