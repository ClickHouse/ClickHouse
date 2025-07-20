#pragma once

#include <Core/Field.h>

#include <Analyzer/ConstantValue.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Columns/IColumn_fwd.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

/** Constant node represents constant value in query tree.
  * Constant value must be representable by Field.
  * Examples: 1, 'constant_string', [1,2,3].
  *
  * Constant node can optionally keep pointer to its source expression.
  */
class ConstantNode;
using ConstantNodePtr = std::shared_ptr<ConstantNode>;

class ConstantNode final : public IQueryTreeNode
{
public:
    /// Construct constant query tree node from constant value and source expression
    explicit ConstantNode(ConstantValue constant_value_, QueryTreeNodePtr source_expression);

    /// Construct constant query tree node from constant value
    explicit ConstantNode(ConstantValue constant_value_);

    /** Construct constant query tree node from column and data type.
      *
      * Throws exception if value cannot be converted to value data type.
      */
    explicit ConstantNode(ColumnPtr constant_column_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from column, data type will be derived from field value
    explicit ConstantNode(ColumnPtr constant_column_);

    /** Construct constant query tree node from field and data type.
      *
      * Throws exception if value cannot be converted to value data type.
      */
    explicit ConstantNode(Field value_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from field, data type will be derived from field value
    explicit ConstantNode(Field value_);

    /// Get constant value
    const ColumnPtr & getColumn() const
    {
        return constant_value.getColumn();
    }

    /// Get constant value
    Field getValue() const
    {
        Field out;
        constant_value.getColumn()->get(0, out);
        return out;
    }

    /// Get constant value string representation
    String getValueStringRepresentation() const;

    /// Returns true if constant node has source expression, false otherwise
    bool hasSourceExpression() const
    {
        return source_expression != nullptr;
    }

    /// Get source expression
    const QueryTreeNodePtr & getSourceExpression() const
    {
        return source_expression;
    }

    /// Get source expression
    QueryTreeNodePtr & getSourceExpression()
    {
        return source_expression;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::CONSTANT;
    }

    DataTypePtr getResultType() const override
    {
        return constant_value.getType();
    }

    /// Check if conversion to AST requires wrapping with _CAST function.
    static bool requiresCastCall(const DataTypePtr & field_type, const DataTypePtr & data_type);

    /// Check if constant is a result of _CAST function constant folding.
    bool receivedFromInitiatorServer() const;

    void setMaskId(size_t id = std::numeric_limits<decltype(mask_id)>::max())
    {
        mask_id = id;
    }

    void convertToNullable() override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    std::pair<String, DataTypePtr> getValueNameAndType(const IColumn::Options & options) const
    {
        return constant_value.getValueNameAndType(options);
    }

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const override;

    void updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const override;

    QueryTreeNodePtr cloneImpl() const override;

    template <typename F>
    std::shared_ptr<ASTLiteral> getCachedAST(const F &ast_generator) const;
    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    ConstantValue constant_value;
    QueryTreeNodePtr source_expression;
    size_t mask_id = 0;

    static constexpr size_t children_size = 0;

    /// Converting to AST maybe costly (for example for large arrays), so we want
    /// to cache it using hash to check for update
    mutable std::shared_ptr<ASTLiteral> cached_ast;
    mutable Hash hash_ast;
};

}
