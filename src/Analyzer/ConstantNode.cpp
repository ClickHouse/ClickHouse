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
#include <Parsers/ASTExpressionList.h>

#include <Interpreters/convertFieldToType.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

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

namespace
{

UInt32 getDecimalFieldScale(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Decimal32:
            return field.safeGet<DecimalField<Decimal32>>().getScale();
        case Field::Types::Decimal64:
            return field.safeGet<DecimalField<Decimal64>>().getScale();
        case Field::Types::Decimal128:
            return field.safeGet<DecimalField<Decimal128>>().getScale();
        case Field::Types::Decimal256:
            return field.safeGet<DecimalField<Decimal256>>().getScale();
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected a decimal field");
    }
}

/// A decimal value has no exact literal syntax in SQL: a bare numeric literal is re-parsed on the
/// receiving side as Float64 and loses precision for high-scale values. Serialize it as its exact
/// textual form cast from a String to a Decimal type wide enough to hold every digit. The
/// String -> Decimal conversion parses the digits exactly, so the value round-trips without loss.
ASTPtr makeExactDecimalLeafAST(const Field & field)
{
    const String text = applyVisitor(FieldVisitorToString(), field);
    const UInt32 scale = getDecimalFieldScale(field);

    size_t digits = 0;
    for (char c : text)
        if (c >= '0' && c <= '9')
            ++digits;

    /// The carrier Decimal type must be wide enough to hold every significant digit.
    const size_t needed_precision = digits > scale ? digits : scale;
    const char * decimal_type_name = "Decimal256";
    if (needed_precision <= 9)
        decimal_type_name = "Decimal32";
    else if (needed_precision <= 18)
        decimal_type_name = "Decimal64";
    else if (needed_precision <= 38)
        decimal_type_name = "Decimal128";

    const String carrier_type_name = String(decimal_type_name) + "(" + std::to_string(scale) + ")";
    return makeASTFunction("_CAST", make_intrusive<ASTLiteral>(text), make_intrusive<ASTLiteral>(carrier_type_name));
}

ASTPtr makeASTFunctionFromList(std::string_view name, ASTs children)
{
    auto function = make_intrusive<ASTFunction>();
    function->name = name;
    function->arguments = make_intrusive<ASTExpressionList>();
    function->children.push_back(function->arguments);
    function->arguments->children = std::move(children);
    return function;
}

/// True if the type is a Decimal or contains one nested anywhere (Array/Tuple/Map/Variant/Nullable/...).
/// Used as a cheap guard to skip materializing the field for the common decimal-free constants.
bool typeContainsDecimal(const IDataType & type)
{
    bool result = false;
    auto check = [&](const IDataType & nested) { result |= WhichDataType(nested).isDecimal(); };
    check(type);
    type.forEachChild(check);
    return result;
}

bool fieldContainsDecimal(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
            return true;
        case Field::Types::Array:
        {
            for (const auto & element : field.safeGet<Array>())
                if (fieldContainsDecimal(element))
                    return true;
            return false;
        }
        case Field::Types::Tuple:
        {
            for (const auto & element : field.safeGet<Tuple>())
                if (fieldContainsDecimal(element))
                    return true;
            return false;
        }
        default:
            return false;
    }
}

/// Rebuild a literal AST from a field, replacing decimal leaves with exact casts and keeping the
/// structure of arrays and tuples, so decimals nested in Array/Tuple/Map round-trip without loss.
ASTPtr fieldToExactLiteralAST(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
            return makeExactDecimalLeafAST(field);
        case Field::Types::Array:
        {
            ASTs elements;
            for (const auto & element : field.safeGet<Array>())
                elements.push_back(fieldToExactLiteralAST(element));
            return makeASTFunctionFromList("array", std::move(elements));
        }
        case Field::Types::Tuple:
        {
            ASTs elements;
            for (const auto & element : field.safeGet<Tuple>())
                elements.push_back(fieldToExactLiteralAST(element));
            return makeASTFunctionFromList("tuple", std::move(elements));
        }
        default:
            return make_intrusive<ASTLiteral>(field);
    }
}

}

ASTPtr ConstantNode::toASTImpl(const ConvertToASTOptions & options) const
{
    static const auto from_column = [](const ConstantNode &node){ return make_intrusive<ASTLiteral>(getFieldFromColumnForASTLiteral(node.constant_value.getColumn(), 0, node.constant_value.getType())); };
    static const auto from_field = [](const ConstantNode &node){ return make_intrusive<ASTLiteral>(node.getValue()); };

    if (options.use_source_expression_for_constants && source_expression)
        return source_expression->toAST(options);

    const auto & constant_value_type = constant_value.getType();

    /// Decimal constants (including decimals nested in Array/Tuple/Map) have no exact literal syntax:
    /// a bare numeric literal is re-parsed as Float64 on the receiving side and rounds. We rebuild
    /// the literal from the same field used by the cast path below, upgrading every decimal leaf to
    /// an exact String -> Decimal cast. The field is produced by getFieldFromColumnForASTLiteral,
    /// which already renders DateTime64 and Variant values in their own exact representation, so only
    /// the lossy decimal leaves are changed.
    /// This must run even when add_cast_for_constants is false (e.g. the RHS of IN/notIn, where casts
    /// are suppressed): a bare decimal in the set would be parsed as Float64 on the shard and round,
    /// so an OR-to-IN rewrite over high-scale Decimal values could filter on rounded constants.
    if (typeContainsDecimal(*constant_value_type))
    {
        if (auto literal_field = getFieldFromColumnForASTLiteral(constant_value.getColumn(), 0, constant_value_type);
            fieldContainsDecimal(literal_field))
        {
            auto exact_ast = fieldToExactLiteralAST(literal_field);
            if (!options.add_cast_for_constants)
                return exact_ast;
            return makeASTFunction("_CAST", std::move(exact_ast), make_intrusive<ASTLiteral>(constant_value_type->getName()));
        }
    }

    if (!options.add_cast_for_constants)
        return getCachedAST(from_column);

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
