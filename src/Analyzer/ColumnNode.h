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
  * During query analysis pass identifier node is resolved into column. See IdentifierNode.h.
  *
  * Examples:
  * SELECT id FROM test_table. id is identifier that must be resolved to column node during query analysis pass.
  * SELECT lambda(x -> x + 1, [1,2,3]). x is identifier inside lambda that must be resolved to column node during query analysis pass.
  *
  * Column node is initialized with column name, type and column source weak pointer.
  * Additional care must be taken during clone to repoint column source to another node if its necessary see IQueryTreeNode.h `clone` method.
  */
class ColumnNode;
using ColumnNodePtr = std::shared_ptr<ColumnNode>;

class ColumnNode final : public IQueryTreeNode
{
public:
    /// Construct column node with column name, type and column source weak pointer.
    explicit ColumnNode(NameAndTypePair column_, QueryTreeNodeWeakPtr column_source_)
        : column(std::move(column_))
        , column_source(std::move(column_source_))
    {}

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

    void dumpTree(WriteBuffer & buffer, size_t indent) const override;

    String getName() const override
    {
        return column.name;
    }

    DataTypePtr getResultType() const override
    {
        return column.type;
    }

protected:
    void updateTreeHashImpl(HashState & hash_state) const override;

    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

    void getPointersToUpdateAfterClone(QueryTreePointersToUpdate & pointers_to_update) override;

private:
    NameAndTypePair column;
    QueryTreeNodeWeakPtr column_source;
};

}
