#include <Analyzer/ConstantNode.h>

#include <Analyzer/FunctionNode.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeDateTime64.h>

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

bool ConstantNode::requiresCastCall() const
{
    const auto & constant_value_literal = constant_value->getValue();
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

    // Add cast if constant was created as a result of constant folding.
    // Constant folding may lead to type transformation and literal on shard
    // may have a different type.
    return need_to_add_cast_function || source_expression != nullptr;
}

bool ConstantNode::receivedFromInitiatorServer() const
{
    if (!hasSourceExpression())
        return false;

    auto * cast_function = getSourceExpression()->as<FunctionNode>();
    if (!cast_function || cast_function->getFunctionName() != "_CAST")
        return false;
    return true;
}

void ConstantNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CONSTANT id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", constant_value: ";
    if (mask_id)
        buffer << "[HIDDEN id: " << mask_id << "]";
    else
        buffer << constant_value->getValue().dump();

    buffer << ", constant_value_type: " << constant_value->getType()->getName();

    if (!mask_id && getSourceExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';
        getSourceExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

void ConstantNode::convertToNullable()
{
    constant_value = std::make_shared<ConstantValue>(constant_value->getValue(), makeNullableSafe(constant_value->getType()));
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);

    if (value_string != rhs_typed.value_string || constant_value->getValue() != rhs_typed.constant_value->getValue())
        return false;

    return !compare_options.compare_types || constant_value->getType()->equals(*rhs_typed.constant_value->getType());
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const
{
    if (compare_options.compare_types)
    {
        auto type_name = constant_value->getType()->getName();
        hash_state.update(type_name.size());
        hash_state.update(type_name);
    }

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
    const auto & constant_value_type = constant_value->getType();
    auto constant_value_ast = std::make_shared<ASTLiteral>(constant_value_literal);

    if (!options.add_cast_for_constants)
        return constant_value_ast;

    if (requiresCastCall())
    {
        /** Value for DateTime64 is Decimal64, which is serialized as a string literal.
          * If we serialize it as is, DateTime64 would be parsed from that string literal, which can be incorrect.
          * For example, DateTime64 cannot be parsed from the short value, like '1', while it's a valid Decimal64 value.
          * It could also lead to ambiguous parsing because we don't know if the string literal represents a date or a Decimal64 literal.
          * For this reason, we use a string literal representing a date instead of a Decimal64 literal.
          */
        if (WhichDataType(constant_value_type->getTypeId()).isDateTime64())
        {
            const auto * date_time_type = typeid_cast<const DataTypeDateTime64 *>(constant_value_type.get());
            DecimalField<Decimal64> decimal_value;
            if (constant_value_literal.tryGet<DecimalField<Decimal64>>(decimal_value))
            {
                WriteBufferFromOwnString ostr;
                writeDateTimeText(decimal_value.getValue(), date_time_type->getScale(), ostr, date_time_type->getTimeZone());
                constant_value_ast = std::make_shared<ASTLiteral>(ostr.str());
            }
        }

        auto constant_type_name_ast = std::make_shared<ASTLiteral>(constant_value_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    return constant_value_ast;
}

}
