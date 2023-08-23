#pragma once

#include <Core/Field.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ConstantValue.h>

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
    explicit ConstantNode(ConstantValuePtr constant_value_, QueryTreeNodePtr source_expression);

    /// Construct constant query tree node from constant value
    explicit ConstantNode(ConstantValuePtr constant_value_);

    /** Construct constant query tree node from field and data type.
      *
      * Throws exception if value cannot be converted to value data type.
      */
    explicit ConstantNode(Field value_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from field, data type will be derived from field value
    explicit ConstantNode(Field value_);

    /// Get constant value
    const Field & getValue() const
    {
        return constant_value->getValue();
    }

    /// Get constant value string representation
    const String & getValueStringRepresentation() const
    {
        return value_string;
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
        return constant_value->getType();
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & hash_state) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    ConstantValuePtr constant_value;
    String value_string;
    QueryTreeNodePtr source_expression;

    static constexpr size_t children_size = 0;
};

}
