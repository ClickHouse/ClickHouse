#include <cstdint>
#include <Analyzer/ConstantNode.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <DataTypes/FieldToDataType.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <Interpreters/convertFieldToType.h>

namespace DB
{

    ConstantNode::ConstantNode(ConstantValue constant_value_, QueryTreeNodePtr source_expression_, bool is_deterministic_)
    : IQueryTreeNode(children_size)
    , constant_value(std::move(constant_value_))
    , is_deterministic(is_deterministic_)
{
    source_expression = std::move(source_expression_);
}

ConstantNode::ConstantNode(ConstantValue constant_value_)
    : ConstantNode(constant_value_, nullptr /*source_expression*/)
{}

ConstantNode::ConstantNode(ColumnConstPtr constant_column_, DataTypePtr value_data_type_)
    : ConstantNode(ConstantValue{constant_column_, value_data_type_})
{}

ConstantNode::ConstantNode(ColumnConstPtr constant_column_)
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
    // Special handling for Bool literals that are stored as UInt64 internally
    // Check if this is a Bool constant based on the data type
    if (isBool(getResultType()) && isInt64OrUInt64FieldType(getValue().getType()))
    {
        // This is a Bool literal stored as UInt64 - generate proper column name
        UInt64 bool_value = getValue().safeGet<UInt64>();
        return bool_value ? "true" : "false";
    }

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

    /// The initiator serializes a folded constant as `_CAST('<value>', '<type>')` with a plain literal inside,
    /// so only that shape means that the constant was received from the initiator. `_CAST(__getScalar('<hash>'), '<type>')`
    /// is different: it is a live expression in the initiator's query tree (for example, a scalar subquery result
    /// cast by the `DistanceTransposedPartialReadsPass` optimization), and the initiator names the result column
    /// after the whole expression. A constant folded from it on a secondary server must be named after its source
    /// expression as well, or the initiator won't find the expected column in blocks received from remote servers.
    const auto & cast_arguments = cast_function->getArguments().getNodes();
    if (!cast_arguments.empty())
    {
        const IQueryTreeNode * cast_argument = cast_arguments.front().get();
        if (const auto * constant_argument = cast_argument->as<ConstantNode>();
            constant_argument && constant_argument->hasSourceExpression())
            cast_argument = constant_argument->getSourceExpression().get();

        if (const auto * function_argument = cast_argument->as<FunctionNode>();
            function_argument && function_argument->getFunctionName() == "__getScalar")
            return false;
    }

    return true;
}

void ConstantNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "CONSTANT id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", constant_value: ";
    if (isMasked())
        buffer << getMaskString();
    else
        buffer << getValue().dump();

    buffer << ", constant_value_type: " << constant_value.getType()->getName();

    if (!isMasked() && getSourceExpression())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "EXPRESSION" << '\n';
        getSourceExpression()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

void ConstantNode::convertToNullable()
{
    /// Use the LowCardinality-aware variant so that a `LowCardinality(T)` key becomes
    /// `LowCardinality(Nullable(T))` rather than being left unchanged (a plain `Nullable`
    /// cannot wrap `LowCardinality`). This keeps the analyzer in sync with `ColumnNode`,
    /// `FunctionNode` and the planner, which all use `makeNullableOrLowCardinalityNullableSafe`
    /// when `group_by_use_nulls` is enabled. Otherwise the declared key type would stay
    /// non-Nullable while the runtime produces a Nullable column, leading to a logical error.
    const auto & column = constant_value.getColumn();
    constant_value
        = {ColumnConst::create(makeNullableOrLowCardinalityNullableSafe(column->getDataColumnPtr()), column->size()),
           makeNullableOrLowCardinalityNullableSafe(constant_value.getType())};
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions /*compare_options*/) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);

    const auto & column = constant_value.getColumn();
    const auto & rhs_column = rhs_typed.constant_value.getColumn();

    return constant_value.getType()->equals(*rhs_typed.constant_value.getType())
           && column->compareAt(0, 0, *rhs_column, 1) == 0;
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state, CompareOptions /*compare_options*/) const
{
    constant_value.getColumn()->updateHashFast(hash_state);
    constant_value.getType()->updateHash(hash_state);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    auto result = std::make_shared<ConstantNode>(constant_value, source_expression, is_deterministic);
    result->mask_id = mask_id;
    return result;
}

template <typename F>
boost::intrusive_ptr<ASTLiteral> ConstantNode::getCachedAST(const F &ast_generator) const
{
    HashState hash_state;
    hash_state.update(getTreeHash());
    /// ast_generator function's address is used as a key to uniquely define generated AST
    hash_state.update(reinterpret_cast<const std::uintptr_t>(&ast_generator));
    auto hash = getSipHash128AsPair(hash_state);

    if (cached_ast && hash == hash_ast)
        return make_intrusive<ASTLiteral>(*cached_ast);

    hash_ast = hash;
    cached_ast = ast_generator(*this);

    return make_intrusive<ASTLiteral>(*cached_ast);
}

ASTPtr ConstantNode::toASTImpl(const ConvertToASTOptions & options) const
{
    static const auto from_column = [](const ConstantNode &node){ return make_intrusive<ASTLiteral>(getFieldFromColumnForASTLiteral(node.constant_value.getColumn(), 0, node.constant_value.getType())); };
    static const auto from_field = [](const ConstantNode &node){ return make_intrusive<ASTLiteral>(node.getValue()); };

    if (options.use_source_expression_for_constants && source_expression)
        return source_expression->toAST(options);

    if (!options.add_cast_for_constants)
        return getCachedAST(from_column);

    const auto & constant_value_type = constant_value.getType();

    // Add cast if constant was created as a result of constant folding.
    // Constant folding may lead to type transformation and literal on shard
    // may have a different type.

    auto requires_cast = [this]()
    {
        try
        {
            auto field_type = applyVisitor(FieldToDataType(), getValue());
            return requiresCastCall(field_type, getResultType());
        }
        catch (...)
        {
            /// FieldToDataType may throw for complex cases like mixed-type arrays.
            /// If we can't determine the natural type, a cast is needed.
            return true;
        }
    };

    if (source_expression != nullptr || requires_cast())
    {
        /// For some types we cannot just get a field from a column, because it can loose type information during serialization/deserialization of the literal.
        /// For example, DateTime64 will return Field with Decimal64 and we won't be able to parse it to DateTine64 back in some cases.
        /// Also for Dynamic and Object types we can lose types information, so we need to create a Field carefully.
        ASTPtr constant_value_ast = getCachedAST(from_column);

        /// A Variant value is serialized as a plain literal of its current member type, while conversion to Variant
        /// is allowed only for types equal by name to one of its members. The literal does not keep the exact member
        /// type (e.g. a `Point` value of `Geometry` becomes a plain tuple whose type is inferred back as
        /// `Tuple(Float64, Float64)`, and a `UInt64` value 42 is inferred back as `UInt8`), so a secondary server
        /// would fail to resolve `_CAST(<literal>, '<variant type>')`. Cast the literal to the exact member type first.
        if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(constant_value_type.get()))
        {
            ColumnPtr column = constant_value.getColumn();
            if (isColumnConst(*column))
                column = assert_cast<const ColumnConst &>(*column).getDataColumnPtr();

            const auto & variant_column = assert_cast<const ColumnVariant &>(*column);
            auto global_discr = variant_column.globalDiscriminatorAt(0);
            if (global_discr != ColumnVariant::NULL_DISCRIMINATOR)
            {
                auto member_type_name_ast = make_intrusive<ASTLiteral>(variant_type->getVariants()[global_discr]->getName());
                constant_value_ast = makeASTFunction("_CAST", std::move(constant_value_ast), std::move(member_type_name_ast));
            }
        }

        auto constant_type_name_ast = make_intrusive<ASTLiteral>(constant_value_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    auto constant_value_ast = getCachedAST(from_field);

    if (isBool(constant_value_type))
        constant_value_ast->value = Field(constant_value_ast->value.safeGet<UInt64>() != 0);

    return constant_value_ast;
}

}
