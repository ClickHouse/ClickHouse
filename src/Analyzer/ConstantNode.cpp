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

// namespace
// {

// ColumnPtr createConstColumn(const Field & value, const DataTypePtr & data_type)
// {
//     std::cerr << "CreateConstColumn " << value.dump() << " data type " << data_type;
//     std::cerr << (data_type ? data_type->getName() : "NULL") << '\n';

//     return data_type->createColumnConst(0, value);
// }

// }

ConstantNode::ConstantNode(ColumnPtr constant_column_, DataTypePtr constant_type_, QueryTreeNodePtr source_expression_)
    : IQueryTreeNode(children_size)
    , constant_column(std::move(constant_column_))
    , constant_type(std::move(constant_type_))
{
    if (!isColumnConst(*constant_column))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConstantNode must be initialized with constant column");

    source_expression = std::move(source_expression_);
}

ConstantNode::ConstantNode(ColumnPtr constant_column_, DataTypePtr constant_type_)
    : ConstantNode(std::move(constant_column_), std::move(constant_type_), {})
{
}

ConstantNode::ConstantNode(ColumnPtr constant_column_)
    : ConstantNode(std::move(constant_column_), applyVisitor(FieldToDataType(), (*constant_column_)[0]))
{
}

ConstantNode::ConstantNode(const Field & constant_value_, DataTypePtr constant_type_, QueryTreeNodePtr source_expression_)
    : ConstantNode(constant_type_->createColumnConst(1, constant_value_), constant_type_, std::move(source_expression_))
{}

ConstantNode::ConstantNode(const Field & constant_value_, DataTypePtr constant_type_)
    : ConstantNode(constant_value_, constant_type_, {})
{}

ConstantNode::ConstantNode(const Field & constant_value_)
    : ConstantNode(constant_value_, applyVisitor(FieldToDataType(), constant_value_))
{}

String ConstantNode::getValueStringRepresentation() const
{
    return applyVisitor(FieldVisitorToString(), getValue());
}

void ConstantNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CONSTANT id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", constant_value: " << (*constant_column)[0].dump();
    buffer << ", constant_value_type: " << constant_type->getName();

    if (getSourceExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';
        getSourceExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);
    return constant_column->compareAt(0, 0, *rhs_typed.constant_column, 0) == 0 && constant_type->equals(*rhs_typed.constant_type);
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state) const
{
    constant_column->updateHashFast(hash_state);

    auto type_name = constant_type->getName();
    hash_state.update(type_name.size());
    hash_state.update(type_name);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    return std::make_shared<ConstantNode>(constant_column, constant_type, source_expression);
}

ASTPtr ConstantNode::toASTImpl(const ConvertToASTOptions & options) const
{
    const auto & constant_value_literal = (*constant_column)[0];
    auto constant_value_ast = std::make_shared<ASTLiteral>(constant_value_literal);

    if (!options.add_cast_for_constants)
        return constant_value_ast;

    bool need_to_add_cast_function = false;
    auto constant_value_literal_type = constant_value_literal.getType();
    WhichDataType constant_value_type(constant_type);

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
        auto constant_type_name_ast = std::make_shared<ASTLiteral>(constant_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    return constant_value_ast;
}

}
