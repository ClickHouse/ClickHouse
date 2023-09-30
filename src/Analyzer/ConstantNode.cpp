#include <Analyzer/ConstantNode.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/FieldToDataType.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Interpreters/convertFieldToType.h>

namespace DB
{

ConstantNode::ConstantNode(ConstantValuePtr constant_value_, QueryTreeNodePtr source_expression_)
    : IQueryTreeNode(children_size)
    , constant_value(std::move(constant_value_))
    , value_string(applyVisitor(FieldVisitorToString(), constant_value->getValue()))
{
    source_expression = std::move(source_expression_);
}

ConstantNode::ConstantNode(ConstantValuePtr constant_value_)
    : ConstantNode(constant_value_, nullptr /*source_expression*/)
{}

ConstantNode::ConstantNode(Field value_, DataTypePtr value_data_type_)
    : ConstantNode(std::make_shared<ConstantValue>(convertFieldToTypeOrThrow(value_, *value_data_type_), value_data_type_))
{}

ConstantNode::ConstantNode(Field value_)
    : ConstantNode(value_, applyVisitor(FieldToDataType(), value_))
{}

void ConstantNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CONSTANT id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", constant_value: " << constant_value->getValue().dump();
    buffer << ", constant_value_type: " << constant_value->getType()->getName();

    if (getSourceExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';
        getSourceExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);
    return *constant_value == *rhs_typed.constant_value && value_string == rhs_typed.value_string;
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state) const
{
    auto type_name = constant_value->getType()->getName();
    hash_state.update(type_name.size());
    hash_state.update(type_name);

    hash_state.update(value_string.size());
    hash_state.update(value_string);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    return std::make_shared<ConstantNode>(constant_value, source_expression);
}

ASTPtr ConstantNode::toASTImpl(const ConvertToASTOptions & options) const
{
    const auto & constant_value_literal = constant_value->getValue();
    auto constant_value_ast = std::make_shared<ASTLiteral>(constant_value_literal);

    if (!options.add_cast_for_constants)
        return constant_value_ast;

    bool need_to_add_cast_function = false;
    auto constant_value_literal_type = constant_value_literal.getType();
    WhichDataType constant_value_type(constant_value->getType());

    switch (constant_value_literal_type)
    {
        case Field::Types::String:
        {
            need_to_add_cast_function = !constant_value_type.isString();
            break;
        }
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64:
        {
            WhichDataType constant_value_field_type(applyVisitor(FieldToDataType(), constant_value_literal));
            need_to_add_cast_function = constant_value_field_type.idx != constant_value_type.idx;
            break;
        }
        case Field::Types::Int128:
        case Field::Types::UInt128:
        case Field::Types::Int256:
        case Field::Types::UInt256:
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
        case Field::Types::AggregateFunctionState:
        case Field::Types::Array:
        case Field::Types::Tuple:
        case Field::Types::Map:
        case Field::Types::UUID:
        case Field::Types::Bool:
        case Field::Types::Object:
        case Field::Types::IPv4:
        case Field::Types::IPv6:
        case Field::Types::Null:
        case Field::Types::CustomType:
        {
            need_to_add_cast_function = true;
            break;
        }
    }

    if (need_to_add_cast_function)
    {
        auto constant_type_name_ast = std::make_shared<ASTLiteral>(constant_value->getType()->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    return constant_value_ast;
}

}
