#include <Analyzer/ConstantNode.h>

#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/FieldToDataType.h>

#include <Parsers/ASTLiteral.h>

namespace DB
{

ConstantNode::ConstantNode(Field value_, DataTypePtr value_data_type_)
    : value(std::move(value_))
    , value_string_dump(applyVisitor(FieldVisitorToString(), value))
    , type(std::move(value_data_type_))
{}

ConstantNode::ConstantNode(Field value_)
    : value(std::move(value_))
    , value_string_dump(applyVisitor(FieldVisitorToString(), value))
    , type(applyVisitor(FieldToDataType(), value))
{}

void ConstantNode::dumpTree(WriteBuffer & buffer, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CONSTANT ";
    writePointerHex(this, buffer);
    buffer << ' ' << value.dump() << " : " << type->getName();
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);
    return value == rhs_typed.value && value_string_dump == rhs_typed.value_string_dump && type->equals(*rhs_typed.type);
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state) const
{
    auto type_name = type->getName();
    hash_state.update(type_name.size());
    hash_state.update(type_name);

    hash_state.update(value_string_dump.size());
    hash_state.update(value_string_dump);
}

ASTPtr ConstantNode::toASTImpl() const
{
    return std::make_shared<ASTLiteral>(value);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    return std::make_shared<ConstantNode>(value, type);
}

}
