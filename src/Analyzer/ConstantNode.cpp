#include <cstdint>
#include <Analyzer/ConstantNode.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnNullable.h>
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
    /// A constant genuinely serialized by the initiator arrives as a _CAST over literals. A _CAST
    /// produced locally by folding a server-context function (e.g. shardNum() -> _CAST(shardNum(),
    /// ...), which folds to a literal on shards but stays symbolic on the initiator) is different:
    /// its argument is either still a live function node, or a folded but non-deterministic
    /// constant that carries that server-context function as its source expression. In that case
    /// the initiator did not fold it, so the shard must name it via its source expression to match
    /// the initiator; treating it as received-from-initiator would bake in the folded literal and
    /// diverge the header (e.g. shard "_CAST(2, ...)" vs initiator "_CAST(shardNum(), ...)").
    /// Deterministic wrapped literals (tuple(0), NULL, ...) are genuine received constants and must
    /// keep being named via the cast, so only non-deterministic source-carrying args are excluded.
    for (const auto & argument : cast_function->getArguments())
    {
        auto * constant_arg = argument->as<ConstantNode>();
        if (!constant_arg)
            return false;
        if (constant_arg->hasSourceExpression() && !constant_arg->isDeterministic())
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
    return std::make_shared<ConstantNode>(constant_value, source_expression, is_deterministic);
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
        auto constant_value_ast = getCachedAST(from_column);
        auto constant_type_name_ast = make_intrusive<ASTLiteral>(constant_value_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    auto constant_value_ast = getCachedAST(from_field);

    if (isBool(constant_value_type))
        constant_value_ast->value = Field(constant_value_ast->value.safeGet<UInt64>() != 0);

    return constant_value_ast;
}

}
