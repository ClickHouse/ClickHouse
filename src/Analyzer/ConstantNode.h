#pragma once

#include <Core/Field.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ConstantValue.h>
#include <Common/FieldVisitorToString.h>
#include "Columns/ColumnNullable.h"
#include <DataTypes/DataTypeNullable.h>

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
    /// Construct constant query tree node from constant column, constant type and source expression
    explicit ConstantNode(ColumnPtr constant_column_, DataTypePtr constant_type_, QueryTreeNodePtr source_expression_);

    /// Construct constant query tree node from constant column and data type
    explicit ConstantNode(ColumnPtr constant_column_, DataTypePtr constant_type_);

    /// Construct constant query tree node from constant column, data type will be derived from constant column value
    explicit ConstantNode(ColumnPtr constant_column_);

    /// Construct constant query tree node from field, constant type and source expression
    explicit ConstantNode(const Field & constant_value_, DataTypePtr constant_type_, QueryTreeNodePtr source_expression_);

    /// Construct constant query tree node from field and constant type
    explicit ConstantNode(const Field & constant_value_, DataTypePtr constant_type_);

    /// Construct constant query tree node from field, data type will be derived from field value
    explicit ConstantNode(const Field & constant_value_);

    /// Get constant column
    const ColumnPtr & getConstantColumn() const
    {
        return constant_column;
    }

    /// Get constant value
    void getValue(Field & out) const
    {
        constant_column->get(0, out);
    }

    /// Get constant value
    Field getValue() const
    {
        Field out;
        constant_column->get(0, out);
        return out;
    }

    /// Get constant value string representation
    String getValueStringRepresentation() const
    {
        return applyVisitor(FieldVisitorToString(), getValue());
    }

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
        return constant_type;
    }

    /// Check if conversion to AST requires wrapping with _CAST function.
    bool requiresCastCall() const;

    /// Check if constant is a result of _CAST function constant folding.
    bool receivedFromInitiatorServer() const;

    void setMaskId(size_t id)
    {
        mask_id = id;
    }

    void convertToNullable() override
    {
        constant_column = makeNullableSafe(constant_column);
        constant_type = makeNullableSafe(constant_type);
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    /// Mutable for lazy evaluation
    mutable ColumnPtr constant_column;
    DataTypePtr constant_type;
    QueryTreeNodePtr source_expression;
    size_t mask_id = 0;

    static constexpr size_t children_size = 0;
};

}
