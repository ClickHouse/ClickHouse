#pragma once

#include <Core/Field.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Constant node represents constant value in query tree.
  * Constant value must be representable by Field.
  * Examples: 1, 'constant_string', [1,2,3].
  */
class ConstantNode;
using ConstantNodePtr = std::shared_ptr<ConstantNode>;

class ConstantNode final : public IQueryTreeNode
{
public:
    /// Construct constant query tree node from field and data type
    explicit ConstantNode(Field value_, DataTypePtr value_data_type_);

    /// Construct constant query tree node from field, data type will be derived from field value
    explicit ConstantNode(Field value_);

    /// Get constant value
    const Field & getConstantValue() const
    {
        return value;
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::CONSTANT;
    }

    void dumpTree(WriteBuffer & buffer, size_t indent) const override;

    String getName() const override
    {
        return value_string_dump;
    }

    DataTypePtr getResultType() const override
    {
        return type;
    }

protected:
    void updateTreeHashImpl(HashState & hash_state) const override;

    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    Field value;
    String value_string_dump;
    DataTypePtr type;
};

}
