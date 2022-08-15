#pragma once

#include <Core/NamesAndTypes.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Column node represents column in query tree.
  * Column must have some column source.
  * Column can be table expression, lambda, subquery.
  * Column source must be valid during column node lifetime.
  *
  * For table ALIAS columns. Column node must contain expression.
  * For ARRAY JOIN join expression column. Column node must contain expression.
  *
  * Additionaly column must be initialized with column name qualification if there are multiple
  * unqualified columns with same name in query scope.
  * Example: SELECT a.id, b.id FROM test_table_join_1 AS a, test_table_join_1.
  * Both columns a.id and b.id have same unqualified name id. And additionally must be initialized
  * with qualification a and b.
  *
  * During query analysis pass identifier node is resolved into column. See IdentifierNode.h.
  *
  * Examples:
  * SELECT id FROM test_table. id is identifier that must be resolved to column node during query analysis pass.
  * SELECT lambda(x -> x + 1, [1,2,3]). x is identifier inside lambda that must be resolved to column node during query analysis pass.
  *
  * Column node is initialized with column name, type and column source weak pointer.
  * In case of ALIAS column node is initialized with column name, type, alias expression and column source weak pointer.
  *
  * Additional care must be taken during clone to repoint column source to another node if its necessary see IQueryTreeNode.h `clone` method.
  */
class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;

class ColumnNode final : public IQueryTreeNode
{
public:
    /// Construct column node with column name, type and column source weak pointer.
    ColumnNode(NameAndTypePair column_, QueryTreeNodeWeakPtr column_source_);

    /// Construct expression column node with column name, type, column expression and column source weak pointer.
    ColumnNode(NameAndTypePair column_, QueryTreeNodePtr expression_node_, QueryTreeNodeWeakPtr column_source_);

    /// Construct column node with column name, type, column name qualification and column source weak pointer.
    ColumnNode(NameAndTypePair column_, String column_name_qualification_, QueryTreeNodeWeakPtr column_source_);

    /// Construct expression column node with column name, type, column name qualification column expression and column source weak pointer.
    ColumnNode(NameAndTypePair column_, String column_name_qualification_, QueryTreeNodePtr expression_node_, QueryTreeNodeWeakPtr column_source_);

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

    bool hasColumnNameQualfication() const
    {
        return !column_name_qualification.empty();
    }

    const String & getColumnQualification() const
    {
        return column_name_qualification;
    }

    /// Get column type
    const DataTypePtr & getColumnType() const
    {
        return column.type;
    }

    bool hasExpression() const
    {
        return children[expression_child_index] != nullptr;
    }

    const QueryTreeNodePtr & getExpression() const
    {
        return children[expression_child_index];
    }

    QueryTreeNodePtr & getExpression()
    {
        return children[expression_child_index];
    }

    /** Get column source.
      * If column source is not valid logical exception is thrown.
      */
    QueryTreeNodePtr getColumnSource() const;

    /// Get column source weak pointer
    QueryTreeNodeWeakPtr getColumnSourceWeak() const
    {
        return column_source;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::COLUMN;
    }

    String getName() const override
    {
        return column.name;
    }

    DataTypePtr getResultType() const override
    {
        return column.type;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

    void getPointersToUpdateAfterClone(QueryTreePointersToUpdate & pointers_to_update) override;

private:
    NameAndTypePair column;
    String column_name_qualification;
    QueryTreeNodeWeakPtr column_source;

    static constexpr size_t expression_child_index = 0;
    static constexpr size_t children_size = expression_child_index + 1;
};

}
