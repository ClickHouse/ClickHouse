#include <Analyzer/ConstantNode.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnNullable.h>
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

ConstantNode::ConstantNode(ConstantValue constant_value_, QueryTreeNodePtr source_expression_)
    : IQueryTreeNode(children_size)
    , constant_value(std::move(constant_value_))
{
    source_expression = std::move(source_expression_);
}

ConstantNode::ConstantNode(ConstantValue constant_value_)
    : ConstantNode(constant_value_, nullptr /*source_expression*/)
{}

ConstantNode::ConstantNode(ColumnPtr constant_column_, DataTypePtr value_data_type_)
    : ConstantNode(ConstantValue{std::move(constant_column_), value_data_type_})
{}

ConstantNode::ConstantNode(ColumnPtr constant_column_)
    : ConstantNode(constant_column_, applyVisitor(FieldToDataType(), (*constant_column_)[0]))
{}

ConstantNode::ConstantNode(Field value_, DataTypePtr value_data_type_)
    : ConstantNode(ConstantValue{convertFieldToTypeOrThrow(value_, *value_data_type_), value_data_type_})
{}

ConstantNode::ConstantNode(Field value_)
    : ConstantNode(value_, applyVisitor(FieldToDataType(), value_))
{}

String ConstantNode::getValueStringRepresentation() const
{
    return applyVisitor(FieldVisitorToString(), getValue());
}

bool ConstantNode::requiresCastCall(const DataTypePtr & field_type, const DataTypePtr & data_type)
{
    WhichDataType which_field_type(field_type);
    if (which_field_type.isNullable() || which_field_type.isArray() || which_field_type.isTuple())
        return true;

    return field_type->getTypeId() != data_type->getTypeId();
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
    {
        if (mask_id == std::numeric_limits<decltype(mask_id)>::max())
            buffer << "[HIDDEN]";
        else
            buffer << "[HIDDEN id: " << mask_id << "]";
    }
    else
        buffer << getValue().dump();

    buffer << ", constant_value_type: " << constant_value.getType()->getName();

    if (!mask_id && getSourceExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';
        getSourceExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

void ConstantNode::convertToNullable()
{
    constant_value = { makeNullableSafe(constant_value.getColumn()), makeNullableSafe(constant_value.getType()) };
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions compare_options) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);

    const auto & column = constant_value.getColumn();
    const auto & rhs_column = rhs_typed.constant_value.getColumn();

    if (compare_options.compare_types)
        return constant_value.getType()->equals(*rhs_typed.constant_value.getType())
               && column->compareAt(0, 0, *rhs_column, 1) == 0;

    if (column->isNullAt(0))
        return rhs_column->isNullAt(0);

    auto not_nullable_type = removeNullable(constant_value.getType());
    auto not_nullable_rhs_type = removeNullable(rhs_typed.constant_value.getType());

    if (!constant_value.getType()->equals(*rhs_typed.constant_value.getType()))
        return false;

    auto not_nullable_column = removeNullable(column);
    auto not_nullable_rhs_column = removeNullable(rhs_column);

    return not_nullable_column->compareAt(0, 0, *not_nullable_rhs_column, 1) == 0;
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state, CompareOptions compare_options) const
{
    constant_value.getColumn()->updateHashFast(hash_state);
    if (compare_options.compare_types)
        constant_value.getType()->updateHash(hash_state);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    return std::make_shared<ConstantNode>(constant_value, source_expression);
}

ASTPtr ConstantNode::toASTImpl(const ConvertToASTOptions & options) const
{
    if (!options.add_cast_for_constants)
        return std::make_shared<ASTLiteral>(getFieldFromColumnForASTLiteral(constant_value.getColumn(), 0, constant_value.getType()));

    const auto & constant_value_type = constant_value.getType();

    // Add cast if constant was created as a result of constant folding.
    // Constant folding may lead to type transformation and literal on shard
    // may have a different type.

    auto requires_cast = [this]()
    {
        const auto & [_, type] = getValueNameAndType();
        return requiresCastCall(type, getResultType());
    };

    if (source_expression != nullptr || requires_cast())
    {
        /// For some types we cannot just get a field from a column, because it can loose type information during serialization/deserialization of the literal.
        /// For example, DateTime64 will return Field with Decimal64 and we won't be able to parse it to DateTine64 back in some cases.
        /// Also for Dynamic and Object types we can loose types information, so we need to create a Field carefully.
        auto constant_value_ast = std::make_shared<ASTLiteral>(getFieldFromColumnForASTLiteral(constant_value.getColumn(), 0, constant_value.getType()));
        auto constant_type_name_ast = std::make_shared<ASTLiteral>(constant_value_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    auto constant_value_ast = std::make_shared<ASTLiteral>(getValue());

    if (isBool(constant_value_type))
        constant_value_ast->custom_type = constant_value_type;

    return constant_value_ast;
}

}
