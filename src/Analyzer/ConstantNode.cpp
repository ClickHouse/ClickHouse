#include <cstdint>
#include <Analyzer/ConstantNode.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
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

#include <DataTypes/DataTypeTuple.h>

#include <limits>
#include <mutex>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr size_t min_deferred_tuple_size = 32;

template <typename T>
bool holdsInteger(const Field & value)
{
    if constexpr (std::is_unsigned_v<T>)
        return value.getType() == Field::Types::UInt64 && value.safeGet<UInt64>() <= std::numeric_limits<T>::max();
    else
        return value.getType() == Field::Types::Int64 && value.safeGet<Int64>() >= std::numeric_limits<T>::min()
            && value.safeGet<Int64>() <= std::numeric_limits<T>::max();
}

/// Whether the size-1 column of `type` built from `value` holds exactly `value`, i.e. `convertFieldToType` leaves it
/// unchanged: an integer of the same signedness that fits (not `Bool`), or a `String`.
bool isPlainElement(const Field & value, const IDataType & type)
{
    if (type.hasCustomName())
        return false;

    switch (type.getTypeId())
    {
        case TypeIndex::UInt8: return holdsInteger<UInt8>(value);
        case TypeIndex::UInt16: return holdsInteger<UInt16>(value);
        case TypeIndex::UInt32: return holdsInteger<UInt32>(value);
        case TypeIndex::UInt64: return value.getType() == Field::Types::UInt64;
        case TypeIndex::Int8: return holdsInteger<Int8>(value);
        case TypeIndex::Int16: return holdsInteger<Int16>(value);
        case TypeIndex::Int32: return holdsInteger<Int32>(value);
        case TypeIndex::Int64: return value.getType() == Field::Types::Int64;
        case TypeIndex::String: return value.getType() == Field::Types::String;
        default: return false;
    }
}

bool canDeferColumn(const Field & value, const IDataType & type)
{
    if (value.getType() != Field::Types::Tuple)
        return false;

    const auto & elements = value.safeGet<Tuple>();
    if (elements.size() < min_deferred_tuple_size)
        return false;

    const auto * tuple_type = typeid_cast<const DataTypeTuple *>(&type);
    if (!tuple_type || tuple_type->hasCustomName() || tuple_type->getElements().size() != elements.size())
        return false;

    const auto & element_types = tuple_type->getElements();
    for (size_t i = 0; i < elements.size(); ++i)
        if (!isPlainElement(elements[i], *element_types[i]))
            return false;

    return true;
}

template <typename T>
void updateHashWithBytes(SipHash & hash, T value)
{
    hash.update(reinterpret_cast<const char *>(&value), sizeof(value));
}

/// Feeds `hash` exactly what `updateHashFast` of the size-1 column of `type` holding the plain element `value`
/// feeds: the value bytes for `ColumnVector`, the offset and the characters for `ColumnString`.
void updateHashAsColumn(SipHash & hash, const Field & value, const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::UInt8: updateHashWithBytes(hash, static_cast<UInt8>(value.safeGet<UInt64>())); return;
        case TypeIndex::UInt16: updateHashWithBytes(hash, static_cast<UInt16>(value.safeGet<UInt64>())); return;
        case TypeIndex::UInt32: updateHashWithBytes(hash, static_cast<UInt32>(value.safeGet<UInt64>())); return;
        case TypeIndex::UInt64: updateHashWithBytes(hash, value.safeGet<UInt64>()); return;
        case TypeIndex::Int8: updateHashWithBytes(hash, static_cast<Int8>(value.safeGet<Int64>())); return;
        case TypeIndex::Int16: updateHashWithBytes(hash, static_cast<Int16>(value.safeGet<Int64>())); return;
        case TypeIndex::Int32: updateHashWithBytes(hash, static_cast<Int32>(value.safeGet<Int64>())); return;
        case TypeIndex::Int64: updateHashWithBytes(hash, value.safeGet<Int64>()); return;
        case TypeIndex::String:
        {
            const auto & string = value.safeGet<String>();
            updateHashWithBytes(hash, static_cast<IColumn::Offset>(string.size()));
            hash.update(string.data(), string.size());
            return;
        }
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected element type {} of a deferred constant", type.getName());
    }
}

}

struct ConstantNode::DeferredValue
{
    DeferredValue(Field && value_, DataTypePtr type_)
        : type(std::move(type_)), value(std::make_shared<const Field>(std::move(value_)))
    {}

    std::shared_ptr<const Field> getValue()
    {
        std::lock_guard lock(mutex);
        return value;
    }

    /// Builds the column on the first call and drops the `Field`, for every node sharing this value.
    const ConstantValue & getMaterialized()
    {
        std::lock_guard lock(mutex);
        if (!materialized)
        {
            materialized.emplace(*value, type);
            value.reset();
        }
        return *materialized;
    }

    const DataTypePtr type;

private:
    std::mutex mutex;
    std::shared_ptr<const Field> value TSA_GUARDED_BY(mutex);
    std::optional<ConstantValue> materialized TSA_GUARDED_BY(mutex);
};

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
    : IQueryTreeNode(children_size)
    , constant_value(makeConstantValue(std::move(value_), std::move(value_data_type_)))
{}

ConstantNode::ConstantNode(Field value_)
    : IQueryTreeNode(children_size)
    , constant_value(makeConstantValue(std::move(value_)))
{}

ConstantNode::ConstantNode(std::shared_ptr<DeferredValue> deferred_value_, QueryTreeNodePtr source_expression_, bool is_deterministic_)
    : IQueryTreeNode(children_size)
    , constant_value(std::move(deferred_value_))
    , source_expression(std::move(source_expression_))
    , is_deterministic(is_deterministic_)
{}

ConstantNode::ValueOrDeferred ConstantNode::makeConstantValue(Field && value, DataTypePtr value_data_type)
{
    if (canDeferColumn(value, *value_data_type))
        return std::make_shared<DeferredValue>(std::move(value), std::move(value_data_type));
    return ConstantValue{convertFieldToTypeOrThrow(value, *value_data_type), std::move(value_data_type)};
}

ConstantNode::ValueOrDeferred ConstantNode::makeConstantValue(Field && value)
{
    DataTypePtr value_data_type = applyVisitor(FieldToDataType(), value);
    return makeConstantValue(std::move(value), std::move(value_data_type));
}

std::shared_ptr<const Field> ConstantNode::tryGetDeferredField() const
{
    if (const auto * deferred = std::get_if<std::shared_ptr<DeferredValue>>(&constant_value))
        return (*deferred)->getValue();
    return nullptr;
}

const ConstantValue & ConstantNode::materializeDeferred() const
{
    return std::get<std::shared_ptr<DeferredValue>>(constant_value)->getMaterialized();
}

DataTypePtr ConstantNode::getResultType() const
{
    if (const auto * deferred = std::get_if<std::shared_ptr<DeferredValue>>(&constant_value))
        return (*deferred)->type;
    return std::get<ConstantValue>(constant_value).getType();
}

String ConstantNode::getValueStringRepresentation() const
{
    if (const auto deferred_field = tryGetDeferredField())
        return applyVisitor(FieldVisitorToString(), *deferred_field);

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

    buffer << ", constant_value_type: " << getResultType()->getName();

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
    const auto & value = getConstantValue();
    const auto & column = value.getColumn();
    ConstantValue nullable_value{
        ColumnConst::create(makeNullableOrLowCardinalityNullableSafe(column->getDataColumnPtr()), column->size()),
        makeNullableOrLowCardinalityNullableSafe(value.getType())};
    constant_value = std::move(nullable_value);
    cached_tree_hash.reset();
}

bool ConstantNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions /*compare_options*/) const
{
    const auto & rhs_typed = assert_cast<const ConstantNode &>(rhs);

    if (!getResultType()->equals(*rhs_typed.getResultType()))
        return false;

    /// With equal types, the elements of two deferred values are `Field`s of the same types.
    const auto field = tryGetDeferredField();
    const auto rhs_field = rhs_typed.tryGetDeferredField();
    if (field && rhs_field)
        return field == rhs_field || *field == *rhs_field;

    const auto & column = getColumn();
    const auto & rhs_column = rhs_typed.getColumn();
    /// `compareAt` returns 0 for any two aggregate function states.
    if (haveDifferentAggregateFunctionStates(*getResultType(), *column, *rhs_typed.getResultType(), *rhs_column))
        return false;

    return column->compareAt(0, 0, *rhs_column, 1) == 0;
}

void ConstantNode::updateTreeHashImpl(HashState & hash_state, CompareOptions /*compare_options*/) const
{
    const DataTypePtr type = getResultType();

    /// `IDataType::updateHash` skips aggregate function parameter values.
    updateHashWithAggregateFunctionNames(*type, hash_state);

    if (const auto deferred_field = tryGetDeferredField())
    {
#if defined(DEBUG_OR_SANITIZER_BUILD)
        /// A temporary column: `getColumn` would drop the values of every node sharing them.
        HashState column_hash_state = hash_state;
        type->createColumnConst(1, *deferred_field)->updateHashFast(column_hash_state);
#endif
        /// `updateHashFast` of a `ColumnConst(ColumnTuple)` hashes its size-1 element columns in order.
        const auto & elements = deferred_field->safeGet<Tuple>();
        const auto & element_types = assert_cast<const DataTypeTuple &>(*type).getElements();
        for (size_t i = 0; i < elements.size(); ++i)
            updateHashAsColumn(hash_state, elements[i], *element_types[i]);
#if defined(DEBUG_OR_SANITIZER_BUILD)
        HashState field_hash_state = hash_state;
        if (getSipHash128AsPair(field_hash_state) != getSipHash128AsPair(column_hash_state))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash of a deferred constant differs from the hash of its column");
#endif
    }
    else
        getColumn()->updateHashFast(hash_state);

    type->updateHash(hash_state);
}

QueryTreeNodePtr ConstantNode::cloneImpl() const
{
    std::shared_ptr<ConstantNode> result;
    if (const auto * deferred = std::get_if<std::shared_ptr<DeferredValue>>(&constant_value))
        result = std::shared_ptr<ConstantNode>(new ConstantNode(*deferred, source_expression, is_deterministic));
    else
        result = std::make_shared<ConstantNode>(std::get<ConstantValue>(constant_value), source_expression, is_deterministic);
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
    /// A deferred value has plain elements only, which the column would give back as the same `Field`.
    static const auto from_column = [](const ConstantNode &node)
    {
        if (const auto deferred_field = node.tryGetDeferredField())
            return make_intrusive<ASTLiteral>(*deferred_field);
        return make_intrusive<ASTLiteral>(getFieldFromColumnForASTLiteral(node.getColumn(), 0, node.getResultType(), /*date_time_as_numbers=*/true));
    };
    static const auto from_column_date_time_as_text = [](const ConstantNode &node)
    {
        if (const auto deferred_field = node.tryGetDeferredField())
            return make_intrusive<ASTLiteral>(*deferred_field);
        return make_intrusive<ASTLiteral>(getFieldFromColumnForASTLiteral(node.getColumn(), 0, node.getResultType(), /*date_time_as_numbers=*/false));
    };
    static const auto from_field = [](const ConstantNode &node){ return make_intrusive<ASTLiteral>(node.getValue()); };

    if (options.use_source_expression_for_constants && source_expression)
        return source_expression->toAST(options);

    const DataTypePtr constant_value_type = getResultType();

    /// `Decimal` constants (including decimals nested in `Array`/`Tuple`/`Map`/`Variant`/`Dynamic`) have no
    /// exact literal syntax: a bare numeric literal is re-parsed as `Float64` on the receiving side and
    /// rounds. A `Variant` value has none either: its literal does not keep the active member type, which
    /// conversion back to `Variant` requires. Rebuild the literal from the column, upgrading every
    /// decimal-backed leaf to an exact `String` -> `Decimal` cast (reconstructed with its own type), then
    /// cast the whole value to the final type. This must run even when `add_cast_for_constants` is false
    /// (e.g. the RHS of `IN`/`notIn`, where casts are suppressed): a bare decimal in the set would be parsed
    /// as `Float64` on the shard and round, so an `OR`-to-`IN` rewrite over high-scale `Decimal` values could
    /// filter on rounded constants.
    if (typeNeedsExactLiteralSerialization(*constant_value_type))
    {
        auto exact_ast = columnConstantToExactLiteralAST(
            getColumn(), 0, constant_value_type, options.date_time_constants_as_numbers);
        if (!options.add_cast_for_constants)
            return exact_ast;
        /// columnConstantToExactLiteralAST already casts a scalar Decimal/DateTime64/Time64 value to its
        /// own type, so skip a redundant identity cast to the same type.
        return makeCastToTypeNameAST(std::move(exact_ast), constant_value_type->getName());
    }

    if (!options.add_cast_for_constants)
    {
        if (options.date_time_constants_as_numbers)
            return getCachedAST(from_column);
        return getCachedAST(from_column_date_time_as_text);
    }

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
        ASTPtr constant_value_ast = options.date_time_constants_as_numbers
            ? getCachedAST(from_column)
            : getCachedAST(from_column_date_time_as_text);

        auto constant_type_name_ast = make_intrusive<ASTLiteral>(constant_value_type->getName());
        return makeASTFunction("_CAST", std::move(constant_value_ast), std::move(constant_type_name_ast));
    }

    auto constant_value_ast = getCachedAST(from_field);

    if (isBool(constant_value_type))
        constant_value_ast->value = Field(constant_value_ast->value.safeGet<UInt64>() != 0);

    return constant_value_ast;
}

}
