#include <DataTypes/UDT/TypeResolver.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <IO/WriteBuffer.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <utility>

#include <absl/container/flat_hash_map.h>

namespace DB::UDT
{
namespace
{

using ErrorCode = TypeResolverError::Code;

[[noreturn]] void fail(ErrorCode code, std::string_view message)
{
    throw TypeResolverError(code, message);
}

UInt64 checkedSize(size_t value, std::string_view message)
{
    if (!std::in_range<UInt64>(value))
        fail(ErrorCode::LimitExceeded, message);
    return static_cast<UInt64>(value);
}

void addProspectively(UInt64 & value, UInt64 addition, UInt64 maximum, std::string_view message)
{
    if (addition > maximum || value > maximum - addition)
        fail(ErrorCode::LimitExceeded, message);
    value += addition;
}

void pushPathComponent(RelativePhysicalTypePath & path, PhysicalTypeChildLocator locator, UInt64 maximum, std::string_view message)
{
    if (path.size() >= maximum)
        fail(ErrorCode::LimitExceeded, message);
    path.push_back(locator);
}

bool binaryLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool isExactDataTypeAST(const ASTPtr & ast) noexcept
{
    return ast && (ast->as<ASTDataType>() || ast->as<ASTTupleDataType>() || ast->as<ASTEnumDataType>());
}

const ASTExpressionList * getArguments(const ASTPtr & ast)
{
    const auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    const auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    const auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    const ASTDataType * any_data_type = data_type ? data_type : (tuple_type ? static_cast<const ASTDataType *>(tuple_type) : enum_type);
    if (!any_data_type)
        fail(ErrorCode::InvalidASTShape, "physical type child is not an exact data-type AST");
    if (any_data_type->children.size() > 1)
        fail(ErrorCode::InvalidASTShape, "data-type AST owns more than one argument list");
    if (any_data_type->children.empty())
        return nullptr;
    const auto * arguments = any_data_type->children.front()->as<ASTExpressionList>();
    if (!arguments)
        fail(ErrorCode::InvalidASTShape, "data-type AST argument owner is not an expression list");
    return arguments;
}

ASTExpressionList * getMutableArguments(ASTPtr & ast)
{
    auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    ASTDataType * any_data_type = data_type ? data_type : (tuple_type ? static_cast<ASTDataType *>(tuple_type) : enum_type);
    if (!any_data_type)
        fail(ErrorCode::InvalidASTShape, "cloned physical type child is not an exact data-type AST");
    if (any_data_type->children.size() > 1)
        fail(ErrorCode::InvalidASTShape, "cloned data-type AST owns more than one argument list");
    if (any_data_type->children.empty())
        return nullptr;
    auto * arguments = any_data_type->children.front()->as<ASTExpressionList>();
    if (!arguments)
        fail(ErrorCode::InvalidASTShape, "cloned data-type AST argument owner is not an expression list");
    return arguments;
}

const ASTExpressionList * getExactMarkerArguments(const ASTDataType & marker)
{
    if (marker.children.size() > 1)
        fail(ErrorCode::InvalidArgumentLineage, "logical-reference marker owns more than one argument list");
    if (marker.children.empty())
        return nullptr;
    const auto * arguments = marker.children.front()->as<ASTExpressionList>();
    if (!arguments)
        fail(ErrorCode::InvalidArgumentLineage, "logical-reference marker argument owner is not an expression list");
    return arguments;
}

void validateValueActualSyntax(const ASTPtr & syntax, const CanonicalTypeArgumentValue & canonical)
{
    const auto * literal = syntax ? syntax->as<ASTLiteral>() : nullptr;
    if (!literal || !literal->children.empty())
        fail(ErrorCode::CanonicalArgumentMismatch, "non-TYPE logical argument syntax is not one exact literal");
    const Field & value = literal->value;
    switch (canonical.kind)
    {
        case ParameterKind::Type: fail(ErrorCode::CanonicalArgumentMismatch, "TYPE logical argument was routed as a value literal");
        case ParameterKind::Bool:
            if (value.getType() != Field::Types::Bool || value.safeGet<bool>() != std::get<bool>(canonical.value))
                fail(ErrorCode::CanonicalArgumentMismatch, "Bool logical argument syntax differs from its canonical value");
            return;
        case ParameterKind::UInt8:
        case ParameterKind::UInt16:
        case ParameterKind::UInt32:
        case ParameterKind::UInt64:
            if (value.getType() != Field::Types::UInt64 || value.safeGet<UInt64>() != std::get<UInt64>(canonical.value))
                fail(ErrorCode::CanonicalArgumentMismatch, "unsigned logical argument syntax differs from its canonical value");
            return;
        case ParameterKind::Int8:
        case ParameterKind::Int16:
        case ParameterKind::Int32:
        case ParameterKind::Int64:
            if (value.getType() != Field::Types::Int64 || value.safeGet<Int64>() != std::get<Int64>(canonical.value))
                fail(ErrorCode::CanonicalArgumentMismatch, "signed logical argument syntax differs from its canonical value");
            return;
        case ParameterKind::String:
            if (value.getType() != Field::Types::String || value.safeGet<String>() != std::get<String>(canonical.value))
                fail(ErrorCode::CanonicalArgumentMismatch, "String logical argument syntax differs from its canonical value");
            return;
    }
    fail(ErrorCode::CanonicalArgumentMismatch, "logical argument has an unknown canonical kind");
}

struct FieldResourceCounters
{
    UInt64 & nodes;
    UInt64 & edges;
    UInt64 & maximum_depth;
};

struct FieldResourceLimits
{
    UInt64 nodes;
    UInt64 edges;
    UInt64 depth;
};

/// ASTLiteral owns a recursive Field graph. Account the complete frozen
/// surface without allocating scratch: a fixed charge for scalar payloads,
/// every nested String/Object key, and both aggregate-state strings.
void chargeLiteralField(
    const Field & field,
    UInt64 depth,
    UInt64 & bytes,
    UInt64 maximum_bytes,
    FieldResourceCounters counters,
    const FieldResourceLimits & limits,
    ErrorCode invalid_shape_code,
    std::string_view message)
{
    if (depth > limits.depth)
        fail(ErrorCode::LimitExceeded, message);
    addProspectively(counters.nodes, 1, limits.nodes, message);
    counters.maximum_depth = std::max(counters.maximum_depth, depth);

    const auto charge_bytes = [&](size_t size)
    { addProspectively(bytes, checkedSize(size, "literal Field payload length does not fit UInt64"), maximum_bytes, message); };
    const auto charge_children = [&](size_t count)
    { addProspectively(counters.edges, checkedSize(count, "literal Field child count does not fit UInt64"), limits.edges, message); };
    const auto next_depth = [&]
    {
        if (depth == std::numeric_limits<UInt64>::max())
            fail(ErrorCode::LimitExceeded, message);
        return depth + 1;
    };

    switch (field.getType())
    {
        case Field::Types::Null: charge_bytes(1); return;
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64: charge_bytes(sizeof(UInt64)); return;
        case Field::Types::String: charge_bytes(field.safeGet<String>().size()); return;
        case Field::Types::Bool: charge_bytes(1); return;
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UUID:
        case Field::Types::IPv6: charge_bytes(16); return;
        case Field::Types::UInt256:
        case Field::Types::Int256: charge_bytes(32); return;
        case Field::Types::Decimal32: charge_bytes(8); return;
        case Field::Types::Decimal64: charge_bytes(12); return;
        case Field::Types::Decimal128: charge_bytes(20); return;
        case Field::Types::Decimal256: charge_bytes(36); return;
        case Field::Types::IPv4: charge_bytes(4); return;
        case Field::Types::Array: {
            const auto & values = field.safeGet<Array>();
            charge_children(values.size());
            for (const auto & child : values)
                chargeLiteralField(child, next_depth(), bytes, maximum_bytes, counters, limits, invalid_shape_code, message);
            return;
        }
        case Field::Types::Tuple: {
            const auto & values = field.safeGet<Tuple>();
            charge_children(values.size());
            for (const auto & child : values)
                chargeLiteralField(child, next_depth(), bytes, maximum_bytes, counters, limits, invalid_shape_code, message);
            return;
        }
        case Field::Types::Map: {
            const auto & entries = field.safeGet<Map>();
            charge_children(entries.size());
            for (const auto & entry : entries)
            {
                if (entry.getType() != Field::Types::Tuple || entry.safeGet<Tuple>().size() != 2)
                    fail(invalid_shape_code, "literal Field Map contains a malformed entry");
                chargeLiteralField(entry, next_depth(), bytes, maximum_bytes, counters, limits, invalid_shape_code, message);
            }
            return;
        }
        case Field::Types::Object: {
            const auto & entries = field.safeGet<Object>();
            charge_children(entries.size());
            for (const auto & [key, child] : entries)
            {
                charge_bytes(key.size());
                chargeLiteralField(child, next_depth(), bytes, maximum_bytes, counters, limits, invalid_shape_code, message);
            }
            return;
        }
        case Field::Types::AggregateFunctionState: {
            const auto & state = field.safeGet<AggregateFunctionStateData>();
            charge_bytes(state.name.size());
            charge_bytes(state.data.size());
            return;
        }
        case Field::Types::CustomType: fail(invalid_shape_code, "CustomType literal is outside the frozen Field surface");
    }
    fail(invalid_shape_code, "literal kind is outside the frozen Field surface");
}

void validateUnaliased(const ASTWithAlias & node, ErrorCode invalid_shape_code)
{
    if (!node.alias.empty() || node.parametrised_alias || node.preferAliasToColumnName())
        fail(invalid_shape_code, "aliases are outside the ParserDataType surface");
}

bool isParserNullsAction(NullsAction action) noexcept
{
    return action == NullsAction::EMPTY || action == NullsAction::RESPECT_NULLS || action == NullsAction::IGNORE_NULLS;
}

/// Reject hidden owned state before clone() can copy it. Side vectors are
/// charged as structural entries because they retain memory without appearing
/// in IAST::children.
UInt64 validateASTNodeSurface(
    const IAST & node,
    ErrorCode invalid_shape_code,
    bool require_physical_type_families,
    UInt64 & structural_entries,
    UInt64 maximum_structural_entries,
    std::string_view limit_message)
{
    const auto charge_entries = [&](size_t count)
    {
        addProspectively(
            structural_entries, checkedSize(count, "AST owned-entry count does not fit UInt64"), maximum_structural_entries, limit_message);
    };

    if (const auto * enumeration = node.as<ASTEnumDataType>())
    {
        if (!enumeration->children.empty() || !BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(enumeration->name))
            fail(invalid_shape_code, "specialized Enum node has an invalid shape or family");
        charge_entries(enumeration->values.size());
        return 0;
    }
    if (const auto * tuple = node.as<ASTTupleDataType>())
    {
        if (!BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(tuple->name) || tuple->children.size() > 1
            || (!tuple->children.empty() && !tuple->children.front()->as<ASTExpressionList>()))
            fail(invalid_shape_code, "specialized Tuple node has an invalid shape or family");
        const size_t argument_count = tuple->children.empty() ? 0 : tuple->children.front()->as<ASTExpressionList>()->children.size();
        if (!tuple->element_names.empty() && tuple->element_names.size() != argument_count)
            fail(invalid_shape_code, "specialized Tuple labels do not match its arguments");
        charge_entries(tuple->element_names.size());
        return 0;
    }
    if (const auto * data_type = node.as<ASTDataType>())
    {
        const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
        if (data_type->children.size() > 1 || (!data_type->children.empty() && !data_type->children.front()->as<ASTExpressionList>()))
            fail(invalid_shape_code, "generic data-type node has an invalid argument owner");
        if (!classification && require_physical_type_families)
            fail(ErrorCode::UnsubstitutedReference, "unknown logical marker would reach DataTypeFactory");
        return 0;
    }
    if (const auto * list = node.as<ASTExpressionList>())
    {
        if (list->getSeparator() != ',')
            fail(invalid_shape_code, "data-type argument list has a noncanonical separator");
        return 0;
    }
    if (const auto * pair = node.as<ASTNameTypePair>())
    {
        if (!pair->type || pair->children.size() != 1 || pair->children.front().get() != pair->type.get())
            fail(invalid_shape_code, "name/type pair has inconsistent ownership");
        return 0;
    }
    if (const auto * object_path = node.as<ASTObjectTypedPathArgument>())
    {
        if (!object_path->type || object_path->children.size() != 1 || object_path->children.front().get() != object_path->type.get())
            fail(invalid_shape_code, "JSON typed-path argument has inconsistent ownership");
        return 0;
    }
    if (const auto * object_argument = node.as<ASTObjectTypeArgument>())
    {
        const std::array<const ASTPtr *, 4> variants{
            &object_argument->path_with_type, &object_argument->skip_path, &object_argument->skip_path_regexp, &object_argument->parameter};
        size_t active = 0;
        const ASTPtr * owner = nullptr;
        for (const ASTPtr * variant : variants)
        {
            if (!*variant)
                continue;
            ++active;
            owner = variant;
        }
        if (active != 1 || !owner || object_argument->children.size() != 1 || object_argument->children.front().get() != owner->get())
            fail(invalid_shape_code, "JSON argument must contain exactly one owned variant");
        return 0;
    }
    if (const auto * function = node.as<ASTFunction>())
    {
        validateUnaliased(*function, invalid_shape_code);
        if (function->parameters || function->window_definition || !function->window_name.empty() || function->isWindowFunction()
            || function->computeAfterWindowFunctions() || function->isLambdaFunction() || function->preferSubqueryToFunctionFormatting()
            || function->noEmptyArgs() || function->isCompoundName() || function->getKind() != ASTFunction::Kind::ORDINARY_FUNCTION
            || !isParserNullsAction(function->getNullsAction())
            || function->children.size() != (function->arguments ? size_t{1} : size_t{0})
            || (function->arguments && function->children.front().get() != function->arguments.get()))
            fail(invalid_shape_code, "function-shaped data-type argument has unsupported state");
        return 0;
    }
    if (const auto * identifier = node.as<ASTIdentifier>())
    {
        validateUnaliased(*identifier, invalid_shape_code);
        const auto semantic_string_bytes = identifier->getParserIdentifierSemanticStringBytes();
        if (!semantic_string_bytes)
            fail(invalid_shape_code, "identifier argument has unsupported or empty state");
        charge_entries(identifier->name_parts.size());
        return checkedSize(*semantic_string_bytes, "identifier semantic String length does not fit UInt64");
    }
    if (const auto * literal = node.as<ASTLiteral>())
    {
        validateUnaliased(*literal, invalid_shape_code);
        if (!literal->children.empty() || !literal->unique_column_name.empty() || literal->getUseLegacyColumnNameOfTuple())
            fail(invalid_shape_code, "literal argument has non-syntax state");
        return 0;
    }
    fail(invalid_shape_code, "AST node kind is outside the ParserDataType surface");
}

void chargeASTNodeStrings(
    const IAST & node,
    UInt64 & bytes,
    UInt64 maximum_bytes,
    FieldResourceCounters field_counters,
    const FieldResourceLimits & field_limits,
    ErrorCode invalid_shape_code,
    bool require_physical_type_families,
    UInt64 & structural_entries,
    UInt64 maximum_structural_entries,
    std::string_view message)
{
    const UInt64 identifier_semantic_string_bytes = validateASTNodeSurface(
        node, invalid_shape_code, require_physical_type_families, structural_entries, maximum_structural_entries, message);
    const auto charge = [&](std::string_view value)
    { addProspectively(bytes, checkedSize(value.size(), "AST owned string length does not fit UInt64"), maximum_bytes, message); };
    if (const auto * enumeration = node.as<ASTEnumDataType>())
    {
        charge(enumeration->name);
        for (const auto & [name, value] : enumeration->values)
        {
            static_cast<void>(value);
            charge(name);
        }
    }
    else if (const auto * tuple = node.as<ASTTupleDataType>())
    {
        charge(tuple->name);
        for (const auto & name : tuple->element_names)
            charge(name);
    }
    else if (const auto * type = node.as<ASTDataType>())
        charge(type->name);
    else if (const auto * pair = node.as<ASTNameTypePair>())
        charge(pair->name);
    else if (const auto * typed_path = node.as<ASTObjectTypedPathArgument>())
        charge(typed_path->path);
    else if (const auto * function = node.as<ASTFunction>())
        charge(function->name);
    else if (const auto * identifier = node.as<ASTIdentifier>())
    {
        charge(identifier->full_name);
        for (const auto & part : identifier->name_parts)
            charge(part);
        addProspectively(bytes, identifier_semantic_string_bytes, maximum_bytes, message);
    }
    else if (const auto * literal = node.as<ASTLiteral>())
        chargeLiteralField(literal->value, 1, bytes, maximum_bytes, field_counters, field_limits, invalid_shape_code, message);
}

struct ASTResourceCounters
{
    UInt64 & nodes;
    UInt64 & edges;
    UInt64 & maximum_depth;
    UInt64 & string_bytes;
    FieldResourceCounters fields;
};

struct ASTResourceLimits
{
    UInt64 nodes;
    UInt64 edges;
    UInt64 depth;
    UInt64 string_bytes;
    FieldResourceLimits fields;
};

/// Counts AST occurrences rather than pointer identities. That is the actual
/// factory work for a DAG, and it prevents a tiny retained marker from hiding
/// a much larger canonical subtree after substitution.
void preflightASTResources(
    const ASTPtr & root,
    const ASTResourceLimits & limits,
    ASTResourceCounters counters,
    ErrorCode invalid_shape_code,
    bool require_physical_type_families,
    std::string_view limit_message)
{
    if (!root)
        fail(invalid_shape_code, "resource preflight reached a null AST root");

    struct Frame
    {
        const IAST * node = nullptr;
        size_t next_child = 0;
    };
    constexpr size_t implementation_maximum_depth = 64;
    std::array<Frame, implementation_maximum_depth> stack{};
    stack[0].node = root.get();
    size_t stack_size = 1;

    while (stack_size != 0)
    {
        auto & frame = stack[stack_size - 1];
        if (frame.next_child == 0)
        {
            addProspectively(counters.nodes, 1, limits.nodes, limit_message);
            addProspectively(
                counters.edges,
                checkedSize(frame.node->children.size(), "AST child count does not fit UInt64"),
                limits.edges,
                limit_message);
            counters.maximum_depth = std::max(counters.maximum_depth, static_cast<UInt64>(stack_size));
            chargeASTNodeStrings(
                *frame.node,
                counters.string_bytes,
                limits.string_bytes,
                counters.fields,
                limits.fields,
                invalid_shape_code,
                require_physical_type_families,
                counters.edges,
                limits.edges,
                limit_message);
        }
        if (frame.next_child == frame.node->children.size())
        {
            --stack_size;
            continue;
        }
        const auto & child = frame.node->children[frame.next_child++];
        if (!child)
            fail(invalid_shape_code, "resource preflight reached a null AST child");
        if (stack_size >= limits.depth || stack_size >= stack.size())
            fail(ErrorCode::LimitExceeded, limit_message);
        for (size_t ancestor = 0; ancestor < stack_size; ++ancestor)
            if (stack[ancestor].node == child.get())
                fail(invalid_shape_code, "resource preflight reached a cyclic AST");
        stack[stack_size++] = {.node = child.get(), .next_child = 0};
    }
}

/// Compares the complete encoding without materializing it. Each writable
/// window is at most the remaining canonical length, so an overlong producer
/// is rejected before it can copy even one byte beyond the admitted budget.
class CanonicalEncodingComparisonWriteBuffer final : public WriteBuffer
{
public:
    CanonicalEncodingComparisonWriteBuffer(char * storage_, size_t storage_size_, std::string_view expected_)
        : WriteBuffer(storage_, initialWindowSize(storage_size_, expected_.size()))
        , storage(storage_)
        , storage_size(storage_size_)
        , expected(expected_)
    {
    }

    ~CanonicalEncodingComparisonWriteBuffer() override
    {
        if (!isFinalized() && !isCanceled())
            cancel();
    }

private:
    static size_t initialWindowSize(size_t storage_size, size_t expected_size)
    {
        if (storage_size == 0)
            fail(ErrorCode::LimitExceeded, "TYPE-actual binary comparison has no stack storage");
        return std::min(storage_size, std::max<size_t>(expected_size, 1));
    }

    void nextImpl() override
    {
        const size_t chunk_size = offset();
        const size_t remaining = expected.size() - compared;
        if (chunk_size > remaining || !std::equal(working_buffer.begin(), working_buffer.begin() + chunk_size, expected.begin() + compared))
            fail(ErrorCode::CanonicalArgumentMismatch, "physicalized TYPE-actual binary encoding differs from its canonical argument");
        compared += chunk_size;

        /// A non-final flush at the exact expected boundary can only be the
        /// producer requesting room for another byte. Reject before granting
        /// that room; an exact producer reaches this boundary through finalize.
        if (!finalizing && compared == expected.size())
            fail(ErrorCode::CanonicalArgumentMismatch, "physicalized TYPE-actual binary encoding is longer than its canonical argument");

        const size_t next_size = std::min(storage_size, std::max<size_t>(expected.size() - compared, 1));
        set(storage, next_size);
    }

    void finalizeImpl() override
    {
        finalizing = true;
        next();
        if (compared != expected.size())
            fail(ErrorCode::CanonicalArgumentMismatch, "physicalized TYPE-actual binary encoding is shorter than its canonical argument");
    }

    char * storage;
    size_t storage_size;
    std::string_view expected;
    size_t compared = 0;
    bool finalizing = false;
};

std::string_view canonicalFamily(const ASTPtr & ast)
{
    BuiltInDataTypeFamilyClassification classification;
    if (const auto * tuple = ast ? ast->as<ASTTupleDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(tuple->name);
    else if (const auto * enumeration = ast ? ast->as<ASTEnumDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(enumeration->name);
    else if (const auto * data_type = ast ? ast->as<ASTDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
    else
        fail(ErrorCode::InvalidASTShape, "type position contains an unsupported exact AST category");

    if (!classification)
        fail(ErrorCode::UnsubstitutedReference, "unknown data-type family was not supplied in the logical-reference side table");
    return classification.family->canonical_creator_name;
}

UInt64 mix(UInt64 value) noexcept
{
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

struct OrdinalPathHash
{
    size_t operator()(const std::vector<UInt32> & path) const noexcept
    {
        UInt64 result = mix(path.size());
        for (const UInt32 ordinal : path)
            result = mix(result ^ ordinal);
        return static_cast<size_t>(result);
    }
};

struct LocatorPathHash
{
    size_t operator()(const RelativePhysicalTypePath & path) const noexcept
    {
        UInt64 result = mix(path.size());
        for (const auto & locator : path)
            result = mix(result ^ (static_cast<UInt64>(locator.kind) << 32) ^ locator.source_ordinal);
        return static_cast<size_t>(result);
    }
};

bool lineagePathLess(const RelativePhysicalTypePath & lhs, const RelativePhysicalTypePath & rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](const PhysicalTypeChildLocator & left, const PhysicalTypeChildLocator & right)
        {
            if (left.source_ordinal != right.source_ordinal)
                return left.source_ordinal < right.source_ordinal;
            return static_cast<UInt8>(left.kind) < static_cast<UInt8>(right.kind);
        });
}

void validateLimits(const TypeResolverLimits & limits)
{
    constexpr TypeResolverLimits implementation_maxima;
    const auto check = [](UInt64 value, UInt64 maximum, std::string_view message)
    {
        if (value == 0 || value > maximum)
            fail(ErrorCode::LimitExceeded, message);
    };
    check(
        limits.maximum_input_references,
        implementation_maxima.maximum_input_references,
        "input-reference limit is outside the implementation domain");
    check(
        limits.maximum_argument_lineage_entries,
        implementation_maxima.maximum_argument_lineage_entries,
        "argument-lineage limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_factory_calls,
        implementation_maxima.maximum_argument_validation_factory_calls,
        "argument-validation factory-call limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_ast_nodes,
        implementation_maxima.maximum_argument_validation_ast_nodes,
        "argument-validation AST-node limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_ast_edges,
        implementation_maxima.maximum_argument_validation_ast_edges,
        "argument-validation AST-edge limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_ast_depth,
        implementation_maxima.maximum_argument_validation_ast_depth,
        "argument-validation AST-depth limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_syntax_bytes,
        implementation_maxima.maximum_argument_validation_syntax_bytes,
        "argument-validation syntax-byte limit is outside the implementation domain");
    check(
        limits.maximum_argument_validation_binary_bytes,
        implementation_maxima.maximum_argument_validation_binary_bytes,
        "argument-validation binary-byte limit is outside the implementation domain");
    check(
        limits.maximum_declaration_ast_nodes,
        implementation_maxima.maximum_declaration_ast_nodes,
        "declaration AST node limit is outside the implementation domain");
    check(
        limits.maximum_declaration_ast_edges,
        implementation_maxima.maximum_declaration_ast_edges,
        "declaration AST edge limit is outside the implementation domain");
    check(
        limits.maximum_declaration_ast_depth,
        implementation_maxima.maximum_declaration_ast_depth,
        "declaration AST depth limit is outside the implementation domain");
    check(
        limits.maximum_declaration_ast_syntax_bytes,
        implementation_maxima.maximum_declaration_ast_syntax_bytes,
        "declaration AST syntax-byte limit is outside the implementation domain");
    check(
        limits.maximum_physical_ast_nodes,
        implementation_maxima.maximum_physical_ast_nodes,
        "physical AST node limit is outside the implementation domain");
    check(
        limits.maximum_physical_ast_edges,
        implementation_maxima.maximum_physical_ast_edges,
        "physical AST edge limit is outside the implementation domain");
    check(
        limits.maximum_physical_ast_depth,
        implementation_maxima.maximum_physical_ast_depth,
        "physical AST depth limit is outside the implementation domain");
    check(
        limits.maximum_physical_ast_syntax_bytes,
        implementation_maxima.maximum_physical_ast_syntax_bytes,
        "physical AST syntax-byte limit is outside the implementation domain");
    check(
        limits.maximum_literal_field_nodes,
        implementation_maxima.maximum_literal_field_nodes,
        "literal Field node limit is outside the implementation domain");
    check(
        limits.maximum_literal_field_edges,
        implementation_maxima.maximum_literal_field_edges,
        "literal Field edge limit is outside the implementation domain");
    check(
        limits.maximum_literal_field_depth,
        implementation_maxima.maximum_literal_field_depth,
        "literal Field depth limit is outside the implementation domain");
    check(
        limits.maximum_path_components,
        implementation_maxima.maximum_path_components,
        "path-component limit is outside the implementation domain");
    check(
        limits.maximum_logical_occurrences,
        implementation_maxima.maximum_logical_occurrences,
        "logical-occurrence limit is outside the implementation domain");
    check(
        limits.maximum_variant_branch_factory_calls,
        implementation_maxima.maximum_variant_branch_factory_calls,
        "Variant branch factory-call limit is outside the implementation domain");
    if (limits.maximum_logical_occurrences > limits.descriptors.maximum_occurrences)
        fail(ErrorCode::LimitExceeded, "resolver occurrence limit exceeds the descriptor occurrence limit");
    try
    {
        validateTemplateSpecializerLimits(limits.specializer);
        validateTypeDescriptorLimits(limits.descriptors);
    }
    catch (const TemplateSpecializerError &)
    {
        fail(ErrorCode::LimitExceeded, "nested template-specializer limits are invalid");
    }
    catch (const DescriptorError &)
    {
        fail(ErrorCode::LimitExceeded, "nested descriptor limits are invalid");
    }
}

void preflightDeclarationAST(const ASTPtr & root, const TypeResolverLimits & limits, TypeResolverStatistics & statistics)
{
    if (!root)
        fail(ErrorCode::InvalidRoot, "declared type AST is null");
    preflightASTResources(
        root,
        {.nodes = limits.maximum_declaration_ast_nodes,
         .edges = limits.maximum_declaration_ast_edges,
         .depth = limits.maximum_declaration_ast_depth,
         .string_bytes = limits.maximum_declaration_ast_syntax_bytes,
         .fields
         = {.nodes = limits.maximum_literal_field_nodes,
            .edges = limits.maximum_literal_field_edges,
            .depth = limits.maximum_literal_field_depth}},
        {.nodes = statistics.declaration_ast_nodes,
         .edges = statistics.declaration_ast_edges,
         .maximum_depth = statistics.maximum_declaration_ast_depth,
         .string_bytes = statistics.declaration_ast_syntax_bytes,
         .fields
         = {.nodes = statistics.literal_field_nodes,
            .edges = statistics.literal_field_edges,
            .maximum_depth = statistics.maximum_literal_field_depth}},
        ErrorCode::InvalidASTShape,
        false,
        "declaration AST resource limit exceeded");
}

DataTypePtr stablePhysicalChild(const DataTypePtr & type, UInt32 ordinal)
{
    if (!type)
        fail(ErrorCode::PhysicalTopologyMismatch, "physical path reached a null IDataType");

    if (type->hasCustomName())
    {
        if (isBool(type))
            fail(ErrorCode::PhysicalTopologyMismatch, "Bool has no encoded type child");
        if (const auto * simple = typeid_cast<const DataTypeCustomSimpleAggregateFunction *>(type->getCustomName()))
        {
            const auto & arguments = simple->getArgumentsDataTypes();
            if (ordinal >= arguments.size())
                fail(ErrorCode::PhysicalTopologyMismatch, "SimpleAggregateFunction child ordinal is out of range");
            return arguments[ordinal];
        }
        if (const auto * nested = typeid_cast<const DataTypeNestedCustomName *>(type->getCustomName()))
        {
            const auto & elements = nested->getElements();
            if (ordinal >= elements.size())
                fail(ErrorCode::PhysicalTopologyMismatch, "Nested child ordinal is out of range");
            return elements[ordinal];
        }
        fail(ErrorCode::PhysicalTopologyMismatch, "opaque custom physical type has no encoded type child");
    }

    switch (type->getTypeId())
    {
        case TypeIndex::Array:
            if (ordinal == 0)
                return assert_cast<const DataTypeArray &>(*type).getNestedType();
            break;
        case TypeIndex::Tuple: {
            const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
            if (ordinal < elements.size())
                return elements[ordinal];
            break;
        }
        case TypeIndex::QBit:
            if (ordinal == 0)
                return assert_cast<const DataTypeQBit &>(*type).getElementType();
            break;
        case TypeIndex::Nullable:
            if (ordinal == 0)
                return assert_cast<const DataTypeNullable &>(*type).getNestedType();
            break;
        case TypeIndex::Function: {
            const auto & function = assert_cast<const DataTypeFunction &>(*type);
            const auto & arguments = function.getArgumentTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            if (ordinal == arguments.size() && function.getReturnType())
                return function.getReturnType();
            break;
        }
        case TypeIndex::LowCardinality:
            if (ordinal == 0)
                return assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType();
            break;
        case TypeIndex::Map: {
            const auto & map = assert_cast<const DataTypeMap &>(*type);
            if (ordinal == 0)
                return map.getKeyType();
            if (ordinal == 1)
                return map.getValueType();
            break;
        }
        case TypeIndex::AggregateFunction: {
            const auto arguments = assert_cast<const DataTypeAggregateFunction &>(*type).getArgumentsDataTypes();
            if (ordinal < arguments.size())
                return arguments[ordinal];
            break;
        }
        case TypeIndex::Object: {
            const auto & typed_paths = assert_cast<const DataTypeObject &>(*type).getTypedPaths();
            if (ordinal >= typed_paths.size())
                break;
            std::vector<std::pair<std::string_view, DataTypePtr>> sorted;
            sorted.reserve(typed_paths.size());
            for (const auto & [path, child] : typed_paths)
                sorted.emplace_back(path, child);
            std::sort(sorted.begin(), sorted.end(), [](const auto & lhs, const auto & rhs) { return binaryLess(lhs.first, rhs.first); });
            return sorted[ordinal].second;
        }
        case TypeIndex::Variant: fail(ErrorCode::PhysicalTopologyMismatch, "Variant requires a normalized branch locator");
        default: break;
    }
    fail(ErrorCode::PhysicalTopologyMismatch, "encoded physical type child ordinal is out of range");
}

ASTPtr stableASTChild(const ASTPtr & ast, UInt32 ordinal)
{
    const std::string_view family = canonicalFamily(ast);
    const auto * arguments = getArguments(ast);
    if (!arguments)
        fail(ErrorCode::PhysicalTopologyMismatch, "physical AST family has no argument list for requested child");

    const auto direct = [&](size_t index) -> ASTPtr
    {
        if (index >= arguments->children.size() || !isExactDataTypeAST(arguments->children[index]))
            fail(ErrorCode::PhysicalTopologyMismatch, "physical AST type child ordinal is out of range");
        return arguments->children[index];
    };

    if (family == "Array" || family == "Nullable" || family == "LowCardinality")
    {
        if (ordinal == 0)
            return direct(0);
        fail(ErrorCode::PhysicalTopologyMismatch, "unary physical AST child is out of range");
    }
    if (family == "Map")
    {
        if (ordinal < 2)
            return direct(ordinal);
        fail(ErrorCode::PhysicalTopologyMismatch, "Map physical AST child is out of range");
    }
    if (family == "QBit")
    {
        if (ordinal == 0)
            return direct(0);
        fail(ErrorCode::PhysicalTopologyMismatch, "QBit physical AST child is out of range");
    }
    if (family == "Tuple" || family == "Nested")
    {
        if (ordinal >= arguments->children.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "Tuple/Nested physical AST child is out of range");
        const auto & child = arguments->children[ordinal];
        if (const auto * pair = child->as<ASTNameTypePair>())
            return pair->type;
        if (family == "Tuple" && isExactDataTypeAST(child))
            return child;
        fail(ErrorCode::PhysicalTopologyMismatch, "Tuple/Nested physical AST child has an invalid wrapper");
    }
    if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
    {
        size_t first_type = 1;
        if (family == "AggregateFunction" && !arguments->children.empty() && arguments->children.front()->as<ASTLiteral>())
            first_type = 2;
        return direct(first_type + ordinal);
    }
    if (family == "JSON")
    {
        std::vector<const ASTObjectTypedPathArgument *> typed_paths;
        for (const auto & child : arguments->children)
        {
            const auto * object_argument = child->as<ASTObjectTypeArgument>();
            if (!object_argument || !object_argument->path_with_type)
                continue;
            const auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!typed || !typed->type)
                fail(ErrorCode::PhysicalTopologyMismatch, "JSON typed path wrapper is malformed");
            typed_paths.push_back(typed);
        }
        std::sort(
            typed_paths.begin(), typed_paths.end(), [](const auto * lhs, const auto * rhs) { return binaryLess(lhs->path, rhs->path); });
        if (ordinal >= typed_paths.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "JSON physical AST child is out of range");
        return typed_paths[ordinal]->type;
    }
    fail(ErrorCode::PhysicalTopologyMismatch, "physical AST family has no stable encoded type child");
}

struct MutableASTTopologyCache
{
    absl::flat_hash_map<const IAST *, std::vector<ASTObjectTypedPathArgument *>> json_typed_paths;
    UInt64 entries = 0;
};

std::vector<ASTObjectTypedPathArgument *> & mutableJSONTypedPaths(ASTPtr & ast, MutableASTTopologyCache & cache, UInt64 maximum_entries)
{
    if (auto found = cache.json_typed_paths.find(ast.get()); found != cache.json_typed_paths.end())
        return found->second;
    auto * arguments = getMutableArguments(ast);
    if (canonicalFamily(ast) != "JSON" || !arguments)
        fail(ErrorCode::InvalidArgumentLineage, "mutable JSON topology cache reached a non-JSON AST");
    size_t typed_count = 0;
    for (const auto & argument : arguments->children)
    {
        const auto * object_argument = argument->as<ASTObjectTypeArgument>();
        if (object_argument && object_argument->path_with_type)
            ++typed_count;
    }
    addProspectively(
        cache.entries,
        checkedSize(typed_count, "JSON TYPE-actual typed-path count does not fit UInt64"),
        maximum_entries,
        "TYPE-actual topology scratch-entry limit exceeded");
    auto [it, inserted] = cache.json_typed_paths.try_emplace(ast.get());
    if (!inserted)
        fail(ErrorCode::InvalidArgumentLineage, "mutable JSON topology cache insertion was inconsistent");
    it->second.reserve(typed_count);
    for (auto & argument : arguments->children)
    {
        auto * object_argument = argument->as<ASTObjectTypeArgument>();
        if (!object_argument || !object_argument->path_with_type)
            continue;
        auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
        if (!typed || !typed->type || typed->children.size() != 1)
            fail(ErrorCode::InvalidArgumentLineage, "JSON TYPE-actual typed path wrapper is malformed");
        it->second.push_back(typed);
    }
    std::sort(it->second.begin(), it->second.end(), [](const auto * lhs, const auto * rhs) { return binaryLess(lhs->path, rhs->path); });
    return it->second;
}

ASTPtr mutableStableASTChild(ASTPtr & ast, UInt32 ordinal, MutableASTTopologyCache & cache, UInt64 maximum_entries)
{
    if (canonicalFamily(ast) != "JSON")
        return stableASTChild(ast, ordinal);
    auto & typed_paths = mutableJSONTypedPaths(ast, cache, maximum_entries);
    if (ordinal >= typed_paths.size())
        fail(ErrorCode::InvalidArgumentLineage, "JSON TYPE-actual child ordinal is out of range");
    return typed_paths[ordinal]->type;
}

void setStableASTChild(
    ASTPtr & ast, UInt32 ordinal, ASTPtr child, MutableASTTopologyCache & topology_cache, UInt64 maximum_topology_entries)
{
    const std::string_view family = canonicalFamily(ast);
    auto * arguments = getMutableArguments(ast);
    if (!arguments)
        fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual physicalization reached a family without arguments");

    const auto direct = [&](size_t index)
    {
        if (index >= arguments->children.size())
            fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual replacement child ordinal is out of range");
        arguments->children[index] = std::move(child);
    };
    if (family == "Array" || family == "Nullable" || family == "LowCardinality" || family == "QBit")
    {
        if (ordinal != 0)
            fail(ErrorCode::InvalidArgumentLineage, "unary TYPE-actual replacement child ordinal is out of range");
        direct(0);
        return;
    }
    if (family == "Map")
    {
        if (ordinal >= 2)
            fail(ErrorCode::InvalidArgumentLineage, "Map TYPE-actual replacement child ordinal is out of range");
        direct(ordinal);
        return;
    }
    if (family == "Tuple" || family == "Nested")
    {
        if (ordinal >= arguments->children.size())
            fail(ErrorCode::InvalidArgumentLineage, "Tuple/Nested TYPE-actual replacement child ordinal is out of range");
        if (auto * pair = arguments->children[ordinal]->as<ASTNameTypePair>())
        {
            if (pair->children.size() != 1)
                fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual named field wrapper is malformed");
            pair->type = std::move(child);
            pair->children[0] = pair->type;
            return;
        }
        if (family == "Tuple")
        {
            direct(ordinal);
            return;
        }
        fail(ErrorCode::InvalidArgumentLineage, "Nested TYPE-actual child lacks a named field wrapper");
    }
    if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
    {
        size_t first_type = 1;
        if (family == "AggregateFunction" && !arguments->children.empty() && arguments->children.front()->as<ASTLiteral>())
            first_type = 2;
        direct(first_type + ordinal);
        return;
    }
    if (family == "JSON")
    {
        auto & typed_paths = mutableJSONTypedPaths(ast, topology_cache, maximum_topology_entries);
        if (ordinal >= typed_paths.size())
            fail(ErrorCode::InvalidArgumentLineage, "JSON TYPE-actual replacement child ordinal is out of range");
        typed_paths[ordinal]->type = std::move(child);
        typed_paths[ordinal]->children[0] = typed_paths[ordinal]->type;
        return;
    }
    fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual replacement path uses an unsupported physical family");
}

void replaceASTAtPath(
    ASTPtr & ast,
    std::span<const PhysicalTypeChildLocator> path,
    const ASTPtr & replacement,
    MutableASTTopologyCache & topology_cache,
    UInt64 maximum_topology_entries)
{
    if (path.empty())
    {
        ast = replacement;
        return;
    }
    const auto & locator = path.front();
    ASTPtr child;
    if (locator.kind == PhysicalTypeChildLocatorKind::StableOrdinal)
        child = mutableStableASTChild(ast, locator.source_ordinal, topology_cache, maximum_topology_entries);
    else if (locator.kind == PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
    {
        auto * arguments = getMutableArguments(ast);
        if (canonicalFamily(ast) != "Variant" || !arguments || locator.source_ordinal >= arguments->children.size())
            fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual Variant replacement source ordinal is out of range");
        child = arguments->children[locator.source_ordinal];
    }
    else
        fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual replacement path contains an unknown locator kind");
    replaceASTAtPath(child, path.subspan(1), replacement, topology_cache, maximum_topology_entries);
    if (locator.kind == PhysicalTypeChildLocatorKind::StableOrdinal)
        setStableASTChild(ast, locator.source_ordinal, std::move(child), topology_cache, maximum_topology_entries);
    else if (locator.kind == PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
    {
        auto * arguments = getMutableArguments(ast);
        arguments->children[locator.source_ordinal] = std::move(child);
    }
    else
        fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual replacement path contains an unknown locator kind");
}

const ASTExpressionList & variantArguments(const ASTPtr & ast)
{
    if (canonicalFamily(ast) != "Variant")
        fail(ErrorCode::PhysicalTopologyMismatch, "normalized Variant locator is attached to a non-Variant AST");
    const auto * arguments = getArguments(ast);
    if (!arguments)
        fail(ErrorCode::PhysicalTopologyMismatch, "Variant AST has no arguments");
    return *arguments;
}

UInt64 statisticsDelta(UInt64 before, UInt64 after)
{
    if (after < before)
        fail(ErrorCode::PhysicalTopologyMismatch, "query specialization memo statistics moved backwards");
    return after - before;
}

TemplateSpecializerStatistics
specializationStatisticsDelta(const TemplateSpecializerStatistics & before, const TemplateSpecializerStatistics & after)
{
    return {
        .resolution_sessions = statisticsDelta(before.resolution_sessions, after.resolution_sessions),
        .specialization_requests = statisticsDelta(before.specialization_requests, after.specialization_requests),
        .distinct_specializations = statisticsDelta(before.distinct_specializations, after.distinct_specializations),
        .specialization_memo_hits = statisticsDelta(before.specialization_memo_hits, after.specialization_memo_hits),
        .definition_lookups = statisticsDelta(before.definition_lookups, after.definition_lookups),
        .maximum_specialization_depth = after.maximum_specialization_depth,
        .distinct_definition_handles = statisticsDelta(before.distinct_definition_handles, after.distinct_definition_handles),
        .memo_key_bytes = statisticsDelta(before.memo_key_bytes, after.memo_key_bytes),
        .template_node_occurrences = statisticsDelta(before.template_node_occurrences, after.template_node_occurrences),
        .constructed_ast_nodes = statisticsDelta(before.constructed_ast_nodes, after.constructed_ast_nodes),
        .constructed_ast_edges = statisticsDelta(before.constructed_ast_edges, after.constructed_ast_edges),
        .maximum_ast_depth = after.maximum_ast_depth,
        .owned_ast_string_bytes = statisticsDelta(before.owned_ast_string_bytes, after.owned_ast_string_bytes),
        .enum_entries = statisticsDelta(before.enum_entries, after.enum_entries),
        .retained_occurrences = statisticsDelta(before.retained_occurrences, after.retained_occurrences),
        .retained_path_components = statisticsDelta(before.retained_path_components, after.retained_path_components),
        .emitted_ast_node_occurrences = statisticsDelta(before.emitted_ast_node_occurrences, after.emitted_ast_node_occurrences),
        .emitted_ast_edges = statisticsDelta(before.emitted_ast_edges, after.emitted_ast_edges),
        .emitted_occurrences = statisticsDelta(before.emitted_occurrences, after.emitted_occurrences),
        .emitted_path_components = statisticsDelta(before.emitted_path_components, after.emitted_path_components),
        .charged_work = statisticsDelta(before.charged_work, after.charged_work),
    };
}

class ResolverSpecializations final
{
public:
    ResolverSpecializations(
        const IAuthorityAdapter & authority_,
        const TemplateSpecializerLimits & limits,
        ProspectiveResourceBudget * query_budget,
        TemplateSpecializer::QueryMemo * query_memo_)
        : authority(authority_)
        , query_memo(query_memo_)
    {
        if (!query_memo)
        {
            local_attempt.emplace(TemplateSpecializer::Attempt::begin(authority_, limits, query_budget));
            return;
        }

        if (!query_budget || !query_memo->usesResourceBudget(*query_budget)
            || query_memo->getAuthorityDatabaseUUID() != authority_.getDatabaseUUID() || query_memo->getLimits() != limits)
        {
            fail(ErrorCode::InvalidReference, "query specialization memo belongs to another budget, authority, or limit domain");
        }
        try
        {
            auto session = authority_.beginResolutionSession();
            if (session.getGeneration() != query_memo->getAuthorityGeneration())
                fail(ErrorCode::InvalidReference, "query specialization memo belongs to another authority generation");
        }
        catch (const TypeResolverError &)
        {
            throw;
        }
        catch (const std::exception &)
        {
            fail(ErrorCode::InvalidReference, "query specialization memo authority generation cannot be verified");
        }
        statistics_before = query_memo->getStatistics();
    }

    TemplateSpecializationID specialize(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments)
    {
        return query_memo ? query_memo->specialize(authority, identity, arguments) : local_attempt->specialize(identity, arguments);
    }

    const ASTPtr & getCanonicalPhysicalAST(TemplateSpecializationID id)
    {
        return query_memo ? query_memo->getCanonicalPhysicalAST(id) : local_attempt->getCanonicalPhysicalAST(id);
    }

    void seal()
    {
        if (query_memo)
            return;
        if (!local_attempt || finished)
            fail(ErrorCode::PhysicalTopologyMismatch, "local specialization batch cannot be sealed more than once");
        finished.emplace(local_attempt->finish());
        local_attempt.reset();
    }

    TemplateSpecializationView getSpecialization(TemplateSpecializationID id) const
    {
        if (query_memo)
            return query_memo->getSpecialization(id);
        if (!finished || id >= finished->specializations.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "specialization ID is outside the sealed local batch");
        const auto & specialization = finished->specializations[id];
        if (specialization.definition_handle_index >= finished->definition_handles.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "specialization definition handle is outside the sealed local batch");
        return {
            .definition_identity = specialization.definition_identity,
            .canonical_arguments = specialization.canonical_arguments,
            .canonical_physical_ast = specialization.canonical_physical_ast,
            .relative_occurrences = specialization.relative_occurrences,
            .definition_handle = finished->definition_handles[specialization.definition_handle_index],
        };
    }

    TemplateSpecializerStatistics getCallStatistics() const
    {
        if (query_memo)
            return specializationStatisticsDelta(statistics_before, query_memo->getStatistics());
        if (!finished)
            fail(ErrorCode::PhysicalTopologyMismatch, "local specialization statistics were requested before sealing");
        return finished->statistics;
    }

private:
    const IAuthorityAdapter & authority;
    TemplateSpecializer::QueryMemo * query_memo = nullptr;
    std::optional<TemplateSpecializer::Attempt> local_attempt;
    std::optional<FinishedTemplateSpecializations> finished;
    TemplateSpecializerStatistics statistics_before;
};

class ActivatedResolver final
{
public:
    ActivatedResolver(
        const ASTPtr & declaration_ast_,
        std::span<const DeclaredTypeReferenceInput> references_,
        const IAuthorityAdapter & authority_,
        const TypeResolverLimits & limits_,
        TypeResolverStatistics & statistics_,
        ProspectiveResourceBudget * query_budget_,
        TemplateSpecializer::QueryMemo * query_memo_)
        : declaration_ast(declaration_ast_)
        , references(references_)
        , authority(authority_)
        , limits(limits_)
        , statistics(statistics_)
        , query_budget(query_budget_)
        , query_memo(query_memo_)
    {
    }

    BoundDeclaredTypeResult resolve();

private:
    struct RootReference
    {
        RelativePhysicalTypePath prefix;
        size_t reference = 0;
    };

    struct PendingOccurrence
    {
        RelativePhysicalTypePath path;
        TemplateSpecializationID specialization = invalid_template_specialization_id;
    };

    struct ResolvedPath
    {
        std::vector<UInt32> ordinals;
        DataTypePtr physical_type;
    };

    enum class VariantBranchState : UInt8
    {
        Mapped,
        Dropped,
        Collapsed,
    };

    struct VariantBranch
    {
        DataTypePtr physical_type;
        String canonical_name;
        VariantBranchState state = VariantBranchState::Mapped;
        UInt32 final_ordinal = 0;
    };

    struct VariantMap
    {
        DataTypePtr final_type;
        std::vector<VariantBranch> branches;
    };

    void indexReferences();
    void validateArgumentLineage();
    void validateCanonicalTypeActuals(ResolverSpecializations & specializations);
    TemplateSpecializationID consumeReference(size_t reference, ResolverSpecializations & specializations);
    void visitType(const ASTPtr & original, ASTPtr & clone, RelativePhysicalTypePath & path, ResolverSpecializations & specializations);
    void visitDirectArgument(
        const ASTExpressionList & original_arguments,
        ASTExpressionList & cloned_arguments,
        size_t argument_index,
        PhysicalTypeChildLocator locator,
        RelativePhysicalTypePath & path,
        ResolverSpecializations & specializations);
    const ResolvedPath & resolvePath(const RelativePhysicalTypePath & path, const ASTPtr & physical_ast, const DataTypePtr & physical_type);
    UInt32 normalizeVariantBranch(const ASTPtr & ast, const DataTypePtr & type, UInt32 source_ordinal);
    VariantMap buildVariantMap(const ASTPtr & ast, const DataTypePtr & type);
    ASTPtr stableASTChildCached(const ASTPtr & ast, UInt32 ordinal);
    DataTypePtr stablePhysicalChildCached(const DataTypePtr & type, UInt32 ordinal);
    void retainNode(const std::vector<UInt32> & path, const DataTypePtr & physical_type);
    void appendPendingOccurrences(
        size_t reference,
        RelativePhysicalTypePath & path,
        const ResolverSpecializations & specializations,
        std::vector<PendingOccurrence> & output,
        UInt64 & output_path_components,
        UInt64 depth);

    const ASTPtr & declaration_ast;
    std::span<const DeclaredTypeReferenceInput> references;
    const IAuthorityAdapter & authority;
    const TypeResolverLimits & limits;
    TypeResolverStatistics & statistics;
    ProspectiveResourceBudget * query_budget = nullptr;
    TemplateSpecializer::QueryMemo * query_memo = nullptr;
    absl::flat_hash_map<const IAST *, size_t> reference_index;
    std::vector<UInt8> matched_references;
    std::vector<UInt8> reference_activation_state;
    std::vector<TemplateSpecializationID> reference_specializations;
    std::vector<size_t> lineage_offsets;
    std::vector<UInt8> replayed_lineage;
    std::vector<RootReference> root_references;
    UInt64 retained_input_path_components = 0;
    UInt64 retained_descriptor_path_elements = 0;
    UInt64 topology_cache_entries = 0;
    absl::flat_hash_map<const IAST *, VariantMap> variant_maps;
    absl::flat_hash_map<const IAST *, std::vector<ASTPtr>> json_ast_children;
    absl::flat_hash_map<const IDataType *, std::vector<DataTypePtr>> json_physical_children;
    absl::flat_hash_map<const IDataType *, DataTypes> aggregate_physical_children;
    absl::flat_hash_map<RelativePhysicalTypePath, ResolvedPath, LocatorPathHash> resolved_paths;
    absl::flat_hash_map<std::vector<UInt32>, DataTypePtr, OrdinalPathHash> bound_nodes;
};

void ActivatedResolver::indexReferences()
{
    if (references.size() > limits.maximum_input_references)
        fail(ErrorCode::LimitExceeded, "logical-reference side table exceeds its input limit");
    statistics.input_references = checkedSize(references.size(), "logical-reference count does not fit UInt64");
    reference_index.reserve(references.size());
    matched_references.assign(references.size(), 0);
    reference_activation_state.assign(references.size(), 0);
    reference_specializations.assign(references.size(), invalid_template_specialization_id);
    for (size_t index = 0; index < references.size(); ++index)
    {
        const auto & reference = references[index];
        if (!reference.reference_node)
            fail(ErrorCode::InvalidReference, "logical-reference side table contains a null AST identity");
        const auto * marker = reference.reference_node->as<ASTDataType>();
        if (!marker)
            fail(ErrorCode::InvalidReference, "logical-reference marker is not an exact generic data-type AST");
        static_cast<void>(getExactMarkerArguments(*marker));
        if (BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(marker->name))
            fail(ErrorCode::InvalidReference, "logical-reference marker name collides with a registered built-in family or alias");
        if (!reference_index.emplace(reference.reference_node, index).second)
            fail(ErrorCode::DuplicateReference, "logical-reference side table contains a duplicate AST identity");
    }
}

void ActivatedResolver::validateArgumentLineage()
{
    std::vector<UInt32> incoming(references.size(), 0);
    lineage_offsets.resize(references.size() + 1);
    UInt64 lineage_entries = 0;
    for (size_t reference = 0; reference < references.size(); ++reference)
    {
        const auto & input = references[reference];
        lineage_offsets[reference] = static_cast<size_t>(lineage_entries);
        addProspectively(
            lineage_entries,
            checkedSize(input.type_argument_lineage.size(), "argument-lineage count does not fit UInt64"),
            limits.maximum_argument_lineage_entries,
            "argument-lineage entry limit exceeded");
        std::optional<UInt16> previous_parameter;
        const RelativePhysicalTypePath * previous_path = nullptr;
        for (const auto & lineage : input.type_argument_lineage)
        {
            if (previous_parameter && lineage.parameter < *previous_parameter)
                fail(ErrorCode::InvalidArgumentLineage, "argument-lineage records are not grouped by formal parameter");
            if (previous_parameter && lineage.parameter == *previous_parameter && !lineagePathLess(*previous_path, lineage.path))
                fail(ErrorCode::DuplicateArgumentLineage, "argument-lineage paths are not in strict physical preorder");
            previous_parameter = lineage.parameter;
            previous_path = &lineage.path;
            for (const auto & locator : lineage.path)
                if (locator.kind != PhysicalTypeChildLocatorKind::StableOrdinal
                    && locator.kind != PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
                    fail(ErrorCode::InvalidArgumentLineage, "argument-lineage path contains an unknown locator kind");
            if (!lineage.reference_node)
                fail(ErrorCode::InvalidArgumentLineage, "argument lineage contains a null logical-reference identity");
            const auto target = reference_index.find(lineage.reference_node);
            if (target == reference_index.end())
                fail(ErrorCode::InvalidArgumentLineage, "argument lineage targets an identity absent from the side table");
            if (++incoming[target->second] != 1)
                fail(ErrorCode::DuplicateArgumentLineage, "one nested logical-reference identity has multiple lineage parents");
            addProspectively(
                retained_input_path_components,
                checkedSize(lineage.path.size(), "argument-lineage path length does not fit UInt64"),
                limits.maximum_path_components,
                "retained input path-component limit exceeded");
        }
    }
    lineage_offsets.back() = static_cast<size_t>(lineage_entries);
    replayed_lineage.assign(static_cast<size_t>(lineage_entries), 0);
    statistics.argument_lineage_entries = lineage_entries;

    /// Detect the caller-supplied reference graph before path-shape checks so
    /// a forged cycle has one precise failure category and cannot recurse into
    /// specialization.
    struct Frame
    {
        size_t reference = 0;
        size_t next_child = 0;
    };
    std::vector<UInt8> colors(references.size(), 0);
    std::vector<Frame> stack;
    stack.reserve(std::min<size_t>(references.size(), static_cast<size_t>(limits.maximum_declaration_ast_depth)));
    for (size_t start = 0; start < references.size(); ++start)
    {
        if (colors[start] != 0)
            continue;
        colors[start] = 1;
        stack.push_back({.reference = start});
        while (!stack.empty())
        {
            auto & frame = stack.back();
            const auto & lineage = references[frame.reference].type_argument_lineage;
            if (frame.next_child == lineage.size())
            {
                colors[frame.reference] = 2;
                stack.pop_back();
                continue;
            }
            const auto target = reference_index.find(lineage[frame.next_child++].reference_node);
            if (target == reference_index.end())
                fail(ErrorCode::InvalidArgumentLineage, "argument lineage targets an identity absent from the side table");
            if (colors[target->second] == 1)
                fail(ErrorCode::ArgumentLineageCycle, "argument-lineage reference graph contains a cycle");
            if (colors[target->second] == 2)
                continue;
            if (stack.size() >= limits.maximum_declaration_ast_depth)
                fail(ErrorCode::LimitExceeded, "argument-lineage depth limit exceeded");
            colors[target->second] = 1;
            stack.push_back({.reference = target->second});
        }
    }

    for (const auto & input : references)
    {
        const auto * marker = input.reference_node->as<ASTDataType>();
        const auto * arguments = getExactMarkerArguments(*marker);
        const auto & actuals = input.canonical_arguments.values();
        if ((!arguments && !actuals.empty()) || (arguments && arguments->children.size() != actuals.size()))
            fail(ErrorCode::InvalidArgumentLineage, "logical-reference syntax and canonical arguments have different arity");
        for (size_t parameter = 0; parameter < actuals.size(); ++parameter)
            if (actuals[parameter].kind != ParameterKind::Type)
                validateValueActualSyntax(arguments->children[parameter], actuals[parameter]);

        for (size_t index = 0; index < input.type_argument_lineage.size(); ++index)
        {
            const auto & lineage = input.type_argument_lineage[index];
            if (lineage.parameter >= actuals.size() || actuals[lineage.parameter].kind != ParameterKind::Type || !arguments)
                fail(ErrorCode::InvalidArgumentLineage, "argument lineage names a missing or non-TYPE formal");
            ASTPtr current = arguments->children[lineage.parameter];
            if (!current || !isExactDataTypeAST(current))
                fail(ErrorCode::InvalidArgumentLineage, "TYPE-argument lineage root is not an exact supported data-type AST");
            for (const auto & locator : lineage.path)
            {
                if (reference_index.contains(current.get()))
                    fail(ErrorCode::InvalidArgumentLineage, "argument lineage crosses an unreported intermediate logical application");
                try
                {
                    if (locator.kind == PhysicalTypeChildLocatorKind::StableOrdinal)
                        current = stableASTChildCached(current, locator.source_ordinal);
                    else if (locator.kind == PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
                    {
                        const auto & variant_arguments = variantArguments(current);
                        if (locator.source_ordinal >= variant_arguments.children.size())
                            fail(ErrorCode::InvalidArgumentLineage, "argument-lineage Variant source ordinal is out of range");
                        current = variant_arguments.children[locator.source_ordinal];
                    }
                    else
                        fail(ErrorCode::InvalidArgumentLineage, "argument-lineage path contains an unknown locator kind");
                }
                catch (const TypeResolverError & error)
                {
                    if (error.code == ErrorCode::LimitExceeded || error.code == ErrorCode::InvalidArgumentLineage)
                        throw;
                    fail(ErrorCode::InvalidArgumentLineage, "argument-lineage path is incompatible with its TYPE-actual syntax");
                }
            }
            if (current.get() != lineage.reference_node)
                fail(ErrorCode::InvalidArgumentLineage, "argument-lineage path does not end at its declared logical-reference identity");
        }
    }
}

TemplateSpecializationID ActivatedResolver::consumeReference(size_t reference, ResolverSpecializations & specializations)
{
    if (reference >= references.size())
        fail(ErrorCode::InvalidArgumentLineage, "logical-reference index is outside the side table");
    if (matched_references[reference] != 0)
        fail(ErrorCode::DuplicateReference, "one caller AST marker occurs more than once in declaration or argument lineage");
    if (reference_activation_state[reference] != 0)
        fail(ErrorCode::ArgumentLineageCycle, "argument-lineage reference graph re-entered an active application");

    matched_references[reference] = 1;
    reference_activation_state[reference] = 1;
    const auto & input = references[reference];
    const TemplateSpecializationID specialization = specializations.specialize(input.definition_identity, input.canonical_arguments);
    reference_specializations[reference] = specialization;
    ++statistics.specialization_requests;
    for (const auto & lineage : input.type_argument_lineage)
    {
        const auto target = reference_index.find(lineage.reference_node);
        if (target == reference_index.end())
            fail(ErrorCode::InvalidArgumentLineage, "argument lineage targets an identity absent from the side table");
        static_cast<void>(consumeReference(target->second, specializations));
    }
    reference_activation_state[reference] = 2;
    return specialization;
}

void ActivatedResolver::validateCanonicalTypeActuals(ResolverSpecializations & specializations)
{
    for (size_t reference = 0; reference < references.size(); ++reference)
    {
        const auto & input = references[reference];
        const auto * marker = input.reference_node->as<ASTDataType>();
        const auto * arguments = getExactMarkerArguments(*marker);
        const auto & actuals = input.canonical_arguments.values();
        for (size_t parameter = 0; parameter < actuals.size(); ++parameter)
        {
            if (!arguments || parameter >= arguments->children.size())
                fail(ErrorCode::InvalidArgumentLineage, "logical argument is absent from logical-reference syntax");
            if (actuals[parameter].kind != ParameterKind::Type)
                continue;
            const ASTPtr & syntax_actual = arguments->children[parameter];
            const auto & canonical = std::get<CanonicalTypeArgument>(actuals[parameter].value);
            if (!std::in_range<UInt16>(parameter))
                fail(ErrorCode::LimitExceeded, "TYPE-actual formal ordinal exceeds UInt16");

            /// Every (caller, formal) context is visited exactly once. Do not
            /// cache by syntax pointer: an AST DAG can reuse one node under
            /// parents with different direct logical lineage.
            addProspectively(
                statistics.argument_validation_factory_calls,
                1,
                limits.maximum_argument_validation_factory_calls,
                "TYPE-actual validation factory-call limit exceeded");
            addProspectively(
                statistics.argument_validation_binary_bytes,
                checkedSize(canonical.getBinaryEncoding().size(), "TYPE-actual binary encoding size does not fit UInt64"),
                limits.maximum_argument_validation_binary_bytes,
                "TYPE-actual validation binary-byte limit exceeded");
            preflightASTResources(
                syntax_actual,
                {.nodes = limits.maximum_argument_validation_ast_nodes,
                 .edges = limits.maximum_argument_validation_ast_edges,
                 .depth = limits.maximum_argument_validation_ast_depth,
                 .string_bytes = limits.maximum_argument_validation_syntax_bytes,
                 .fields
                 = {.nodes = limits.maximum_literal_field_nodes,
                    .edges = limits.maximum_literal_field_edges,
                    .depth = limits.maximum_literal_field_depth}},
                {.nodes = statistics.argument_validation_ast_nodes,
                 .edges = statistics.argument_validation_ast_edges,
                 .maximum_depth = statistics.maximum_argument_validation_ast_depth,
                 .string_bytes = statistics.argument_validation_syntax_bytes,
                 .fields
                 = {.nodes = statistics.literal_field_nodes,
                    .edges = statistics.literal_field_edges,
                    .maximum_depth = statistics.maximum_literal_field_depth}},
                ErrorCode::InvalidArgumentLineage,
                false,
                "TYPE-actual source AST resource limit exceeded");

            ASTPtr physicalized = syntax_actual->clone();
            MutableASTTopologyCache mutable_topology_cache;
            const auto & lineage = input.type_argument_lineage;
            const auto first = std::lower_bound(
                lineage.begin(),
                lineage.end(),
                static_cast<UInt16>(parameter),
                [](const DeclaredTypeArgumentLineageInput & item, UInt16 formal) { return item.parameter < formal; });
            for (auto nested = first; nested != lineage.end() && nested->parameter == parameter; ++nested)
            {
                const auto target = reference_index.find(nested->reference_node);
                if (target == reference_index.end() || reference_specializations[target->second] == invalid_template_specialization_id)
                    fail(ErrorCode::InvalidArgumentLineage, "TYPE-actual physicalization reached an unspecialized logical application");
                replaceASTAtPath(
                    physicalized,
                    nested->path,
                    specializations.getCanonicalPhysicalAST(reference_specializations[target->second]),
                    mutable_topology_cache,
                    limits.maximum_argument_validation_ast_nodes);
            }

            /// A tiny marker may expand to a much larger canonical subtree.
            /// Bound that final graph, including strings, before the auxiliary
            /// factory sees it; source-syntax accounting alone is insufficient.
            preflightASTResources(
                physicalized,
                {.nodes = limits.maximum_argument_validation_ast_nodes,
                 .edges = limits.maximum_argument_validation_ast_edges,
                 .depth = limits.maximum_argument_validation_ast_depth,
                 .string_bytes = limits.maximum_argument_validation_syntax_bytes,
                 .fields
                 = {.nodes = limits.maximum_literal_field_nodes,
                    .edges = limits.maximum_literal_field_edges,
                    .depth = limits.maximum_literal_field_depth}},
                {.nodes = statistics.argument_validation_physical_ast_nodes,
                 .edges = statistics.argument_validation_physical_ast_edges,
                 .maximum_depth = statistics.maximum_argument_validation_physical_ast_depth,
                 .string_bytes = statistics.argument_validation_physical_syntax_bytes,
                 .fields
                 = {.nodes = statistics.literal_field_nodes,
                    .edges = statistics.literal_field_edges,
                    .maximum_depth = statistics.maximum_literal_field_depth}},
                ErrorCode::InvalidArgumentLineage,
                true,
                "physicalized TYPE-actual AST resource limit exceeded");

            DataTypePtr actual_type;
            try
            {
                actual_type = DataTypeFactory::instance().get(physicalized);
            }
            catch (const Exception &)
            {
                fail(ErrorCode::InvalidArgumentLineage, "physicalized TYPE-actual syntax is not accepted by DataTypeFactory");
            }
            if (!actual_type->equals(*canonical.getPhysicalType()))
                fail(ErrorCode::CanonicalArgumentMismatch, "physicalized TYPE-actual syntax differs from its canonical argument");
            std::array<char, 4'096> comparison_storage{};
            CanonicalEncodingComparisonWriteBuffer comparison_buffer(
                comparison_storage.data(), comparison_storage.size(), canonical.getBinaryEncoding());
            encodeCanonicalDataType(actual_type, comparison_buffer);
            comparison_buffer.finalize();
        }
    }
}

void ActivatedResolver::visitDirectArgument(
    const ASTExpressionList & original_arguments,
    ASTExpressionList & cloned_arguments,
    size_t argument_index,
    PhysicalTypeChildLocator locator,
    RelativePhysicalTypePath & path,
    ResolverSpecializations & specializations)
{
    if (argument_index >= original_arguments.children.size() || argument_index >= cloned_arguments.children.size())
        fail(ErrorCode::InvalidASTShape, "physical type argument ordinal is out of range");
    pushPathComponent(path, locator, limits.maximum_path_components, "declaration traversal path-component limit exceeded");
    visitType(original_arguments.children[argument_index], cloned_arguments.children[argument_index], path, specializations);
    path.pop_back();
}

void ActivatedResolver::visitType(
    const ASTPtr & original, ASTPtr & clone, RelativePhysicalTypePath & path, ResolverSpecializations & specializations)
{
    if (!original || !clone)
        fail(ErrorCode::InvalidASTShape, "type traversal reached a null AST");

    if (const auto reference = reference_index.find(original.get()); reference != reference_index.end())
    {
        addProspectively(
            retained_input_path_components,
            checkedSize(path.size(), "logical root path length does not fit UInt64"),
            limits.maximum_path_components,
            "retained input path-component limit exceeded");
        const TemplateSpecializationID specialization = consumeReference(reference->second, specializations);
        clone = specializations.getCanonicalPhysicalAST(specialization);
        root_references.push_back({.prefix = path, .reference = reference->second});
        return;
    }

    if (!isExactDataTypeAST(original) || !isExactDataTypeAST(clone))
        fail(ErrorCode::InvalidASTShape, "physical type position is not an exact supported data-type AST");
    const std::string_view family = canonicalFamily(original);
    const auto * original_arguments = getArguments(original);
    auto * cloned_arguments = getMutableArguments(clone);
    if (static_cast<bool>(original_arguments) != static_cast<bool>(cloned_arguments)
        || (original_arguments && original_arguments->children.size() != cloned_arguments->children.size()))
        fail(ErrorCode::InvalidASTShape, "cloned data-type AST changed its argument shape");

    if (family == "Array" || family == "Nullable" || family == "LowCardinality")
    {
        if (!original_arguments || original_arguments->children.size() != 1)
            fail(ErrorCode::InvalidASTShape, "unary physical family has an invalid argument count");
        visitDirectArgument(
            *original_arguments, *cloned_arguments, 0, {PhysicalTypeChildLocatorKind::StableOrdinal, 0}, path, specializations);
        return;
    }
    if (family == "Map")
    {
        if (!original_arguments || original_arguments->children.size() != 2)
            fail(ErrorCode::InvalidASTShape, "Map has an invalid argument count");
        for (UInt32 ordinal = 0; ordinal < 2; ++ordinal)
            visitDirectArgument(
                *original_arguments,
                *cloned_arguments,
                ordinal,
                {PhysicalTypeChildLocatorKind::StableOrdinal, ordinal},
                path,
                specializations);
        return;
    }
    if (family == "QBit")
    {
        if (!original_arguments || (original_arguments->children.size() != 2 && original_arguments->children.size() != 3))
            fail(ErrorCode::InvalidASTShape, "QBit has an invalid argument count");
        visitDirectArgument(
            *original_arguments, *cloned_arguments, 0, {PhysicalTypeChildLocatorKind::StableOrdinal, 0}, path, specializations);
        return;
    }
    if (family == "Variant")
    {
        if (!original_arguments || original_arguments->children.empty())
            fail(ErrorCode::InvalidASTShape, "Variant has no source branches");
        for (size_t index = 0; index < original_arguments->children.size(); ++index)
        {
            if (!std::in_range<UInt32>(index))
                fail(ErrorCode::LimitExceeded, "Variant source ordinal exceeds UInt32");
            visitDirectArgument(
                *original_arguments,
                *cloned_arguments,
                index,
                {PhysicalTypeChildLocatorKind::VariantNormalizedBranch, static_cast<UInt32>(index)},
                path,
                specializations);
        }
        return;
    }
    if (family == "Tuple" || family == "Nested")
    {
        if (!original_arguments)
        {
            if (family == "Nested")
                fail(ErrorCode::InvalidASTShape, "Nested has no fields");
            return;
        }
        for (size_t index = 0; index < original_arguments->children.size(); ++index)
        {
            if (!std::in_range<UInt32>(index))
                fail(ErrorCode::LimitExceeded, "Tuple/Nested child ordinal exceeds UInt32");
            const PhysicalTypeChildLocator locator{PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(index)};
            const auto * original_pair = original_arguments->children[index]->as<ASTNameTypePair>();
            auto * cloned_pair = cloned_arguments->children[index]->as<ASTNameTypePair>();
            if (original_pair || cloned_pair)
            {
                if (!original_pair || !cloned_pair || !original_pair->type || !cloned_pair->type || original_pair->children.size() != 1
                    || cloned_pair->children.size() != 1)
                    fail(ErrorCode::InvalidASTShape, "Tuple/Nested named field wrapper is malformed");
                ASTPtr cloned_type = cloned_pair->type;
                pushPathComponent(path, locator, limits.maximum_path_components, "declaration traversal path-component limit exceeded");
                visitType(original_pair->type, cloned_type, path, specializations);
                path.pop_back();
                cloned_pair->type = cloned_type;
                cloned_pair->children[0] = std::move(cloned_type);
            }
            else
            {
                if (family == "Nested")
                    fail(ErrorCode::InvalidASTShape, "Nested accepts only named type fields");
                visitDirectArgument(*original_arguments, *cloned_arguments, index, locator, path, specializations);
            }
        }
        return;
    }
    if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
    {
        if (!original_arguments || original_arguments->children.empty())
            fail(ErrorCode::InvalidASTShape, "aggregate-function type has no function argument");
        size_t first_type = 1;
        if (family == "AggregateFunction" && original_arguments->children.front()->as<ASTLiteral>())
            first_type = 2;
        if (first_type > original_arguments->children.size())
            fail(ErrorCode::InvalidASTShape, "aggregate-function type metadata exceeds its argument list");
        for (size_t index = first_type; index < original_arguments->children.size(); ++index)
        {
            const size_t raw_ordinal = index - first_type;
            if (!std::in_range<UInt32>(raw_ordinal))
                fail(ErrorCode::LimitExceeded, "aggregate-function type child ordinal exceeds UInt32");
            visitDirectArgument(
                *original_arguments,
                *cloned_arguments,
                index,
                {PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(raw_ordinal)},
                path,
                specializations);
        }
        return;
    }
    if (family == "JSON")
    {
        if (!original_arguments)
            return;
        struct TypedPath
        {
            std::string_view path_name;
            size_t argument_index = 0;
        };
        std::vector<TypedPath> typed_paths;
        addProspectively(
            topology_cache_entries,
            checkedSize(original_arguments->children.size(), "JSON argument count does not fit UInt64"),
            limits.maximum_declaration_ast_edges,
            "AST topology scratch-entry limit exceeded");
        typed_paths.reserve(original_arguments->children.size());
        for (size_t index = 0; index < original_arguments->children.size(); ++index)
        {
            const auto * object_argument = original_arguments->children[index]->as<ASTObjectTypeArgument>();
            if (!object_argument || !object_argument->path_with_type)
                continue;
            const auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!typed || !typed->type)
                fail(ErrorCode::InvalidASTShape, "JSON typed path wrapper is malformed");
            typed_paths.push_back({.path_name = typed->path, .argument_index = index});
        }
        std::sort(
            typed_paths.begin(),
            typed_paths.end(),
            [](const auto & lhs, const auto & rhs) { return binaryLess(lhs.path_name, rhs.path_name); });
        for (size_t ordinal = 0; ordinal < typed_paths.size(); ++ordinal)
        {
            if (ordinal != 0 && typed_paths[ordinal - 1].path_name == typed_paths[ordinal].path_name)
                fail(ErrorCode::InvalidASTShape, "JSON contains duplicate typed paths");
            if (!std::in_range<UInt32>(ordinal))
                fail(ErrorCode::LimitExceeded, "JSON typed-path ordinal exceeds UInt32");
            const size_t argument_index = typed_paths[ordinal].argument_index;
            const auto * original_object = original_arguments->children[argument_index]->as<ASTObjectTypeArgument>();
            auto * cloned_object = cloned_arguments->children[argument_index]->as<ASTObjectTypeArgument>();
            const auto * original_typed = original_object->path_with_type->as<ASTObjectTypedPathArgument>();
            auto * cloned_typed = cloned_object && cloned_object->path_with_type
                ? cloned_object->path_with_type->as<ASTObjectTypedPathArgument>()
                : nullptr;
            if (!cloned_object || !cloned_typed || !cloned_typed->type || cloned_typed->children.size() != 1)
                fail(ErrorCode::InvalidASTShape, "cloned JSON typed path wrapper is malformed");
            ASTPtr cloned_type = cloned_typed->type;
            pushPathComponent(
                path,
                {PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(ordinal)},
                limits.maximum_path_components,
                "declaration traversal path-component limit exceeded");
            visitType(original_typed->type, cloned_type, path, specializations);
            path.pop_back();
            cloned_typed->type = cloned_type;
            cloned_typed->children[0] = std::move(cloned_type);
        }
        return;
    }
    /// Every other registered family has no type child in the complete binary
    /// encoding. A side-table marker hidden in a value parameter remains
    /// unmatched and is rejected after traversal.
}

ActivatedResolver::VariantMap ActivatedResolver::buildVariantMap(const ASTPtr & ast, const DataTypePtr & type)
{
    const auto * variant = typeid_cast<const DataTypeVariant *>(type.get());
    if (!variant)
        fail(ErrorCode::PhysicalTopologyMismatch, "Variant locator reached a non-Variant IDataType");
    const auto & arguments = variantArguments(ast);
    VariantMap result;
    result.final_type = type;
    const UInt64 branch_count = checkedSize(arguments.children.size(), "Variant source branch count does not fit UInt64");
    if (branch_count > limits.maximum_variant_branch_factory_calls - statistics.variant_branch_factory_calls)
        fail(ErrorCode::LimitExceeded, "Variant branch factory-call limit exceeded");
    result.branches.reserve(arguments.children.size());
    for (const auto & source_ast : arguments.children)
    {
        addProspectively(
            statistics.variant_branch_factory_calls,
            1,
            limits.maximum_variant_branch_factory_calls,
            "Variant branch factory-call limit exceeded");
        auto source_type = DataTypeFactory::instance().get(source_ast);
        String canonical_name = source_type->getName();
        result.branches.push_back({.physical_type = std::move(source_type), .canonical_name = std::move(canonical_name)});
    }

    std::vector<UInt32> ordered_branches;
    ordered_branches.reserve(result.branches.size());
    for (size_t index = 0; index < result.branches.size(); ++index)
    {
        auto & branch = result.branches[index];
        if (isNothing(branch.physical_type))
        {
            branch.state = VariantBranchState::Dropped;
            continue;
        }
        if (!std::in_range<UInt32>(index))
            fail(ErrorCode::LimitExceeded, "Variant source branch index exceeds UInt32");
        ordered_branches.push_back(static_cast<UInt32>(index));
    }
    std::sort(
        ordered_branches.begin(),
        ordered_branches.end(),
        [&](UInt32 lhs, UInt32 rhs) { return binaryLess(result.branches[lhs].canonical_name, result.branches[rhs].canonical_name); });
    for (size_t first = 0; first < ordered_branches.size();)
    {
        size_t last = first + 1;
        while (last < ordered_branches.size()
               && result.branches[ordered_branches[first]].canonical_name == result.branches[ordered_branches[last]].canonical_name)
            ++last;
        if (last - first != 1)
        {
            for (size_t index = first; index < last; ++index)
                result.branches[ordered_branches[index]].state = VariantBranchState::Collapsed;
        }
        else
        {
            auto & branch = result.branches[ordered_branches[first]];
            const auto discriminator = variant->tryGetVariantDiscriminator(branch.canonical_name);
            if (!discriminator || *discriminator >= variant->getVariants().size()
                || !variant->getVariants()[*discriminator]->equals(*branch.physical_type))
                fail(ErrorCode::PhysicalTopologyMismatch, "Variant factory normalization cannot be reproduced exactly");
            branch.final_ordinal = static_cast<UInt32>(*discriminator);
        }
        first = last;
    }
    return result;
}

ASTPtr ActivatedResolver::stableASTChildCached(const ASTPtr & ast, UInt32 ordinal)
{
    if (canonicalFamily(ast) != "JSON")
        return stableASTChild(ast, ordinal);
    auto it = json_ast_children.find(ast.get());
    if (it == json_ast_children.end())
    {
        const auto * arguments = getArguments(ast);
        if (!arguments)
            fail(ErrorCode::PhysicalTopologyMismatch, "JSON physical AST has no typed-path argument list");
        std::vector<const ASTObjectTypedPathArgument *> sorted;
        size_t typed_count = 0;
        for (const auto & child : arguments->children)
        {
            const auto * object_argument = child->as<ASTObjectTypeArgument>();
            if (object_argument && object_argument->path_with_type)
                ++typed_count;
        }
        addProspectively(
            topology_cache_entries,
            checkedSize(typed_count, "JSON AST typed-path count does not fit UInt64"),
            limits.maximum_declaration_ast_edges,
            "AST topology scratch-entry limit exceeded");
        sorted.reserve(typed_count);
        for (const auto & child : arguments->children)
        {
            const auto * object_argument = child->as<ASTObjectTypeArgument>();
            if (!object_argument || !object_argument->path_with_type)
                continue;
            const auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!typed || !typed->type)
                fail(ErrorCode::PhysicalTopologyMismatch, "JSON typed path wrapper is malformed");
            sorted.push_back(typed);
        }
        std::sort(sorted.begin(), sorted.end(), [](const auto * lhs, const auto * rhs) { return binaryLess(lhs->path, rhs->path); });
        std::vector<ASTPtr> children;
        children.reserve(sorted.size());
        for (const auto * typed : sorted)
            children.push_back(typed->type);
        it = json_ast_children.emplace(ast.get(), std::move(children)).first;
    }
    if (ordinal >= it->second.size())
        fail(ErrorCode::PhysicalTopologyMismatch, "JSON physical AST child is out of range");
    return it->second[ordinal];
}

DataTypePtr ActivatedResolver::stablePhysicalChildCached(const DataTypePtr & type, UInt32 ordinal)
{
    if (!type->hasCustomName() && type->getTypeId() == TypeIndex::Object)
    {
        auto it = json_physical_children.find(type.get());
        if (it == json_physical_children.end())
        {
            const auto & typed_paths = assert_cast<const DataTypeObject &>(*type).getTypedPaths();
            std::vector<std::pair<std::string_view, DataTypePtr>> sorted;
            addProspectively(
                topology_cache_entries,
                checkedSize(typed_paths.size(), "JSON physical typed-path count does not fit UInt64"),
                limits.maximum_declaration_ast_edges,
                "physical topology scratch-entry limit exceeded");
            sorted.reserve(typed_paths.size());
            for (const auto & [path, child] : typed_paths)
                sorted.emplace_back(path, child);
            std::sort(sorted.begin(), sorted.end(), [](const auto & lhs, const auto & rhs) { return binaryLess(lhs.first, rhs.first); });
            std::vector<DataTypePtr> children;
            children.reserve(sorted.size());
            for (auto & [path, child] : sorted)
            {
                static_cast<void>(path);
                children.push_back(std::move(child));
            }
            it = json_physical_children.emplace(type.get(), std::move(children)).first;
        }
        if (ordinal >= it->second.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "JSON physical child is out of range");
        return it->second[ordinal];
    }
    if (!type->hasCustomName() && type->getTypeId() == TypeIndex::AggregateFunction)
    {
        auto it = aggregate_physical_children.find(type.get());
        if (it == aggregate_physical_children.end())
        {
            const auto & arguments = assert_cast<const DataTypeAggregateFunction &>(*type).getArgumentsDataTypes();
            addProspectively(
                topology_cache_entries,
                checkedSize(arguments.size(), "AggregateFunction physical child count does not fit UInt64"),
                limits.maximum_declaration_ast_edges,
                "physical topology scratch-entry limit exceeded");
            it = aggregate_physical_children.emplace(type.get(), arguments).first;
        }
        if (ordinal >= it->second.size())
            fail(ErrorCode::PhysicalTopologyMismatch, "AggregateFunction physical child is out of range");
        return it->second[ordinal];
    }
    return stablePhysicalChild(type, ordinal);
}

UInt32 ActivatedResolver::normalizeVariantBranch(const ASTPtr & ast, const DataTypePtr & type, UInt32 source_ordinal)
{
    auto it = variant_maps.find(ast.get());
    if (it == variant_maps.end())
        it = variant_maps.emplace(ast.get(), buildVariantMap(ast, type)).first;
    else if (!it->second.final_type->equals(*type))
        fail(ErrorCode::PhysicalTopologyMismatch, "one canonical Variant AST maps to inconsistent physical types");
    if (source_ordinal >= it->second.branches.size())
        fail(ErrorCode::PhysicalTopologyMismatch, "Variant source branch ordinal is out of range");
    const auto & branch = it->second.branches[source_ordinal];
    if (branch.state == VariantBranchState::Dropped)
        fail(ErrorCode::VariantBranchDropped, "logical type occurrence was lowered through a Variant(Nothing) branch");
    if (branch.state == VariantBranchState::Collapsed)
        fail(ErrorCode::VariantBranchCollapsed, "logical type occurrence was lowered through collapsed equal Variant branches");
    return branch.final_ordinal;
}

void ActivatedResolver::retainNode(const std::vector<UInt32> & path, const DataTypePtr & physical_type)
{
    const auto found = bound_nodes.find(path);
    if (found != bound_nodes.end())
    {
        if (!found->second->equals(*physical_type))
            fail(ErrorCode::PhysicalTopologyMismatch, "one normalized physical path resolves to inconsistent subtrees");
        return;
    }
    if (bound_nodes.size() >= limits.descriptors.maximum_nodes)
        fail(ErrorCode::LimitExceeded, "bound physical node limit exceeded");
    bound_nodes.emplace(path, physical_type);
}

void ActivatedResolver::appendPendingOccurrences(
    size_t reference,
    RelativePhysicalTypePath & path,
    const ResolverSpecializations & specializations,
    std::vector<PendingOccurrence> & output,
    UInt64 & output_path_components,
    UInt64 depth)
{
    if (depth > limits.maximum_declaration_ast_depth)
        fail(ErrorCode::LimitExceeded, "argument-lineage expansion depth limit exceeded");
    if (reference >= references.size() || reference_specializations[reference] == invalid_template_specialization_id)
        fail(ErrorCode::PhysicalTopologyMismatch, "argument-lineage expansion reached an unspecialized reference");
    const TemplateSpecializationID specialization_id = reference_specializations[reference];
    const auto specialization = specializations.getSpecialization(specialization_id);
    if (specialization.relative_occurrences.empty())
        fail(ErrorCode::PhysicalTopologyMismatch, "a declared specialization emitted no logical lineage events");

    const auto append_path = [&](const RelativePhysicalTypePath & suffix)
    {
        if (suffix.size() > limits.maximum_path_components || path.size() > limits.maximum_path_components - suffix.size())
            fail(ErrorCode::LimitExceeded, "expanded logical path-component limit exceeded");
        path.reserve(path.size() + suffix.size());
        path.insert(path.end(), suffix.begin(), suffix.end());
    };

    for (const auto & event : specialization.relative_occurrences)
    {
        const size_t before_event = path.size();
        append_path(event.path);
        if (event.kind == RelativeLogicalTypeOccurrenceKind::Specialization)
        {
            static_cast<void>(specializations.getSpecialization(event.source_ordinal));
            addProspectively(statistics.logical_occurrences, 1, limits.maximum_logical_occurrences, "logical occurrence limit exceeded");
            addProspectively(
                output_path_components,
                checkedSize(path.size(), "expanded logical path length does not fit UInt64"),
                limits.maximum_path_components,
                "logical path-component limit exceeded");
            output.push_back({.path = path, .specialization = event.source_ordinal});
        }
        else if (event.kind == RelativeLogicalTypeOccurrenceKind::TypeArgument)
        {
            if (event.source_ordinal > std::numeric_limits<UInt16>::max())
                fail(ErrorCode::PhysicalTopologyMismatch, "TYPE-argument lineage event formal ordinal exceeds UInt16");
            const UInt16 parameter = static_cast<UInt16>(event.source_ordinal);
            const auto & input = references[reference];
            const auto & actuals = input.canonical_arguments.values();
            if (parameter >= actuals.size() || actuals[parameter].kind != ParameterKind::Type)
                fail(ErrorCode::PhysicalTopologyMismatch, "TYPE-argument lineage event names a missing or non-TYPE actual");
            const auto first = std::lower_bound(
                input.type_argument_lineage.begin(),
                input.type_argument_lineage.end(),
                parameter,
                [](const DeclaredTypeArgumentLineageInput & lineage, UInt16 formal) { return lineage.parameter < formal; });
            for (auto lineage = first; lineage != input.type_argument_lineage.end() && lineage->parameter == parameter; ++lineage)
            {
                const size_t lineage_index
                    = lineage_offsets[reference] + static_cast<size_t>(std::distance(input.type_argument_lineage.begin(), lineage));
                if (lineage_index >= replayed_lineage.size())
                    fail(ErrorCode::PhysicalTopologyMismatch, "argument-lineage replay index is outside the validated input");
                replayed_lineage[lineage_index] = 1;
                const size_t before_lineage = path.size();
                append_path(lineage->path);
                const auto target = reference_index.find(lineage->reference_node);
                if (target == reference_index.end())
                    fail(ErrorCode::InvalidArgumentLineage, "argument lineage targets an identity absent from the side table");
                appendPendingOccurrences(target->second, path, specializations, output, output_path_components, depth + 1);
                path.resize(before_lineage);
            }
        }
        else
            fail(ErrorCode::PhysicalTopologyMismatch, "specialization emitted an unknown logical-lineage event kind");
        path.resize(before_event);
    }
}

const ActivatedResolver::ResolvedPath &
ActivatedResolver::resolvePath(const RelativePhysicalTypePath & path, const ASTPtr & physical_ast, const DataTypePtr & physical_type)
{
    if (path.size() > limits.descriptors.maximum_path_depth)
        fail(ErrorCode::LimitExceeded, "logical occurrence path exceeds the descriptor depth limit");
    if (const auto found = resolved_paths.find(path); found != resolved_paths.end())
        return found->second;

    ASTPtr current_ast = physical_ast;
    DataTypePtr current_type = physical_type;
    std::vector<UInt32> ordinals;
    ordinals.reserve(path.size());
    std::vector<DataTypePtr> physical_types;
    physical_types.reserve(path.size() + 1);
    physical_types.push_back(current_type);
    for (const auto & locator : path)
    {
        UInt32 ordinal = locator.source_ordinal;
        ASTPtr next_ast;
        if (locator.kind == PhysicalTypeChildLocatorKind::StableOrdinal)
        {
            next_ast = stableASTChildCached(current_ast, locator.source_ordinal);
            current_type = stablePhysicalChildCached(current_type, locator.source_ordinal);
        }
        else if (locator.kind == PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
        {
            const auto & arguments = variantArguments(current_ast);
            if (locator.source_ordinal >= arguments.children.size())
                fail(ErrorCode::PhysicalTopologyMismatch, "Variant source AST ordinal is out of range");
            next_ast = arguments.children[locator.source_ordinal];
            ordinal = normalizeVariantBranch(current_ast, current_type, locator.source_ordinal);
            current_type = assert_cast<const DataTypeVariant &>(*current_type).getVariant(ordinal);
        }
        else
            fail(ErrorCode::PhysicalTopologyMismatch, "logical path contains an unknown locator kind");
        ordinals.push_back(ordinal);
        physical_types.push_back(current_type);
        current_ast = std::move(next_ast);
    }

    /// Plan every new retained prefix before copying any of them into the
    /// bound-node map. A depth-D path retains O(D^2) path elements, so checking
    /// only the final path would allocate first and fail much later in the
    /// descriptor builder.
    UInt64 additional_nodes = 0;
    UInt64 additional_path_elements = 0;
    std::vector<UInt32> prefix;
    prefix.reserve(ordinals.size());
    for (size_t depth = 0; depth < physical_types.size(); ++depth)
    {
        if (depth != 0)
            prefix.push_back(ordinals[depth - 1]);
        if (bound_nodes.contains(prefix))
            continue;
        addProspectively(additional_nodes, 1, limits.descriptors.maximum_nodes, "bound physical node limit exceeded");
        addProspectively(
            additional_path_elements,
            checkedSize(prefix.size(), "bound physical path length does not fit UInt64"),
            limits.descriptors.maximum_edges,
            "bound physical path-element limit exceeded");
    }

    const UInt64 existing_nodes = checkedSize(bound_nodes.size(), "bound physical node count does not fit UInt64");
    if (existing_nodes > limits.descriptors.maximum_nodes || additional_nodes > limits.descriptors.maximum_nodes - existing_nodes)
        fail(ErrorCode::LimitExceeded, "bound physical node limit exceeded");
    const UInt64 resulting_nodes = existing_nodes + additional_nodes;
    if (resulting_nodes != 0 && resulting_nodes - 1 > limits.descriptors.maximum_edges)
        fail(ErrorCode::LimitExceeded, "bound physical edge limit exceeded");
    addProspectively(
        retained_descriptor_path_elements,
        additional_path_elements,
        limits.descriptors.maximum_edges,
        "bound descriptor path-element limit exceeded");

    prefix.clear();
    for (size_t depth = 0; depth < physical_types.size(); ++depth)
    {
        if (depth != 0)
            prefix.push_back(ordinals[depth - 1]);
        retainNode(prefix, physical_types[depth]);
    }

    auto [inserted, unused] = resolved_paths.emplace(path, ResolvedPath{.ordinals = std::move(ordinals), .physical_type = current_type});
    static_cast<void>(unused);
    return inserted->second;
}

BoundDeclaredTypeResult ActivatedResolver::resolve()
{
    if (!isExactDataTypeAST(declaration_ast))
        fail(ErrorCode::InvalidRoot, "declared type root is not an exact supported data-type AST");
    preflightDeclarationAST(declaration_ast, limits, statistics);
    indexReferences();
    validateArgumentLineage();
    root_references.reserve(std::min<size_t>(references.size(), 64));

    ResolverSpecializations specializations(authority, limits.specializer, query_budget, query_memo);
    ASTPtr physical_ast = declaration_ast->clone();
    RelativePhysicalTypePath root_path;
    visitType(declaration_ast, physical_ast, root_path, specializations);
    for (const UInt8 matched : matched_references)
        if (matched == 0)
            fail(ErrorCode::UnreachableReference, "logical-reference side-table entry is not reachable as a physical type child");

    /// Per-source and per-specialization checks do not bound their composed
    /// depth or total retained strings. Check the fully spliced declaration
    /// before any final DataTypeFactory work.
    preflightASTResources(
        physical_ast,
        {.nodes = limits.maximum_physical_ast_nodes,
         .edges = limits.maximum_physical_ast_edges,
         .depth = limits.maximum_physical_ast_depth,
         .string_bytes = limits.maximum_physical_ast_syntax_bytes,
         .fields
         = {.nodes = limits.maximum_literal_field_nodes,
            .edges = limits.maximum_literal_field_edges,
            .depth = limits.maximum_literal_field_depth}},
        {.nodes = statistics.physical_ast_nodes,
         .edges = statistics.physical_ast_edges,
         .maximum_depth = statistics.maximum_physical_ast_depth,
         .string_bytes = statistics.physical_ast_syntax_bytes,
         .fields
         = {.nodes = statistics.literal_field_nodes,
            .edges = statistics.literal_field_edges,
            .maximum_depth = statistics.maximum_literal_field_depth}},
        ErrorCode::InvalidASTShape,
        true,
        "combined physical AST resource limit exceeded");
    validateCanonicalTypeActuals(specializations);

    specializations.seal();
    statistics.specializer = specializations.getCallStatistics();

    std::vector<PendingOccurrence> pending_occurrences;
    pending_occurrences.reserve(std::min<size_t>(references.size(), 64));
    UInt64 output_path_components = 0;
    for (const auto & root : root_references)
    {
        RelativePhysicalTypePath path = root.prefix;
        appendPendingOccurrences(root.reference, path, specializations, pending_occurrences, output_path_components, 1);
    }
    for (const UInt8 replayed : replayed_lineage)
        if (replayed == 0)
            fail(
                ErrorCode::UnreachableArgumentLineage,
                "nested logical TYPE actual is erased by the realized template and cannot be represented in the path-bound sidecar");
    if (pending_occurrences.empty())
        fail(ErrorCode::PhysicalTopologyMismatch, "activated resolution produced no logical occurrences");

    auto physical_type = DataTypeFactory::instance().get(physical_ast);
    statistics.physical_factory_calls = 1;

    absl::flat_hash_map<TemplateSpecializationID, InstantiatedTypeDescriptor::Ptr> instantiated;
    instantiated.reserve(std::min<UInt64>(pending_occurrences.size(), limits.descriptors.maximum_descriptors));
    std::vector<Definition::Ptr> definition_handles;
    definition_handles.reserve(std::min<UInt64>(pending_occurrences.size(), limits.descriptors.maximum_descriptors));
    std::vector<BoundDeclaredTypeOccurrenceInput> occurrences;
    occurrences.reserve(pending_occurrences.size());
    for (const auto & pending : pending_occurrences)
    {
        if (pending.path.size() > limits.descriptors.maximum_path_depth)
            fail(ErrorCode::LimitExceeded, "logical occurrence path exceeds the descriptor depth limit");
        addProspectively(
            retained_descriptor_path_elements,
            checkedSize(pending.path.size(), "logical occurrence path length does not fit UInt64"),
            limits.descriptors.maximum_edges,
            "bound descriptor path-element limit exceeded");
    }
    UInt32 logical_preorder = 0;
    for (const auto & pending : pending_occurrences)
    {
        const auto & resolved = resolvePath(pending.path, physical_ast, physical_type);

        auto descriptor_it = instantiated.find(pending.specialization);
        const bool inserted = descriptor_it == instantiated.end();
        if (inserted)
        {
            if (instantiated.size() >= limits.descriptors.maximum_descriptors)
                fail(ErrorCode::LimitExceeded, "instantiated descriptor count exceeds its limit");
            descriptor_it = instantiated.try_emplace(pending.specialization).first;
        }
        auto & descriptor = descriptor_it->second;
        const auto specialization = specializations.getSpecialization(pending.specialization);
        if (!descriptor)
        {
            if (!inserted)
                fail(ErrorCode::PhysicalTopologyMismatch, "one memoized specialization lost its instantiated descriptor");
            descriptor = InstantiatedTypeDescriptor::create(
                specialization.definition_handle, specialization.canonical_arguments, resolved.physical_type, limits.descriptors);
            definition_handles.push_back(specialization.definition_handle);
        }
        else if (!descriptor->getPhysicalType()->equals(*resolved.physical_type))
            fail(ErrorCode::PhysicalTopologyMismatch, "one memoized specialization resolves to inconsistent physical subtrees");
        occurrences.push_back(
            {.type_child_ordinals = resolved.ordinals, .logical_descriptor = descriptor, .logical_preorder = logical_preorder++});
    }

    std::vector<BoundDeclaredTypeNodeInput> nodes;
    nodes.reserve(bound_nodes.size());
    for (auto & [path, subtree] : bound_nodes)
        nodes.push_back({.type_child_ordinals = path, .physical_type = std::move(subtree)});
    statistics.bound_nodes = checkedSize(nodes.size(), "bound node count does not fit UInt64");
    auto tree = BoundDeclaredTypeTree::build(std::move(nodes), std::move(occurrences), std::move(definition_handles), limits.descriptors);
    return BoundDeclaredTypeResult::withLogicalTree(std::move(tree));
}
}

TypeResolverError::TypeResolverError(Code code_, std::string_view message)
    : std::runtime_error(String("User-defined type resolution failed: ") + String(message))
    , code(code_)
{
}

BoundDeclaredTypeResult TypeResolver::resolve(
    const ASTPtr & declaration_ast,
    std::span<const DeclaredTypeReferenceInput> references,
    const IAuthorityAdapter & authority,
    const TypeResolverLimits & limits,
    TypeResolverStatistics * statistics,
    ProspectiveResourceBudget * query_budget,
    TemplateSpecializer::QueryMemo * query_memo)
{
    if (references.empty())
    {
        /// This is the entire ordinary-type path. Do not construct an activated
        /// resolver, validate UDT-only limits, or touch the adapter.
        auto result = BoundDeclaredTypeResult::physicalOnly(DataTypeFactory::instance().get(declaration_ast));
        if (statistics)
        {
            TypeResolverStatistics built_in_statistics;
            built_in_statistics.physical_factory_calls = 1;
            *statistics = built_in_statistics;
        }
        return result;
    }

    validateLimits(limits);
    TypeResolverStatistics local_statistics;
    ActivatedResolver resolver(declaration_ast, references, authority, limits, local_statistics, query_budget, query_memo);
    auto result = resolver.resolve();
    if (statistics)
        *statistics = local_statistics;
    return result;
}

}
