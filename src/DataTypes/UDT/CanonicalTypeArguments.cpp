#include <DataTypes/UDT/CanonicalTypeArguments.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/IDataType.h>

#include <Common/Exception.h>

#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <ranges>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
extern const int TYPE_MISMATCH;
}

namespace DB::UDT
{
namespace
{

constexpr UInt64 maximum_implementation_ast_nodes = 1ULL << 20;
constexpr UInt64 maximum_implementation_ast_edges = 4ULL << 20;
constexpr UInt64 maximum_implementation_ast_depth = 64;
constexpr UInt64 maximum_implementation_field_nodes = 1ULL << 20;
constexpr UInt64 maximum_implementation_field_edges = 4ULL << 20;
constexpr UInt64 maximum_implementation_field_depth = 256;
constexpr UInt64 maximum_implementation_string_bytes = 64ULL << 20;
constexpr UInt64 maximum_implementation_enum_entries = 1ULL << 20;
constexpr UInt64 maximum_implementation_qbit_materialized_streams = 1ULL << 16;

bool isSupportedCanonicalCustomName(std::string_view name) noexcept
{
    return name == "Point" || name == "Ring" || name == "Polygon" || name == "MultiPoint" || name == "LineString"
        || name == "MultiLineString" || name == "MultiPolygon" || name == "Geometry";
}

[[noreturn]] void invalidTypeArgument(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid canonical TYPE argument: {}", message);
}

[[noreturn]] void typeArgumentLimit(std::string_view message)
{
    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical TYPE argument limit exceeded: {}", message);
}

[[noreturn]] void invalidCanonicalArguments(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid canonical user-defined type arguments: {}", message);
}

void addProspectively(UInt64 & current, UInt64 amount, UInt64 maximum, std::string_view description)
{
    if (amount > maximum || current > maximum - amount)
        typeArgumentLimit(description);
    current += amount;
}

UInt64 checkedSize(std::size_t value, std::string_view description)
{
    if (!std::in_range<UInt64>(value))
        typeArgumentLimit(description);
    return static_cast<UInt64>(value);
}

void validateTypeArgumentLimits(const CanonicalTypeArgumentLimits & limits)
{
    if (limits.maximum_ast_nodes == 0 || limits.maximum_ast_edges == 0 || limits.maximum_ast_depth == 0 || limits.maximum_field_nodes == 0
        || limits.maximum_field_edges == 0 || limits.maximum_field_depth == 0 || limits.maximum_owned_string_bytes == 0
        || limits.maximum_enum_entries == 0 || limits.maximum_qbit_materialized_streams == 0)
        invalidTypeArgument("every structural limit must be nonzero");
    if (limits.maximum_ast_nodes > maximum_implementation_ast_nodes || limits.maximum_ast_edges > maximum_implementation_ast_edges
        || limits.maximum_ast_depth > maximum_implementation_ast_depth || limits.maximum_field_nodes > maximum_implementation_field_nodes
        || limits.maximum_field_edges > maximum_implementation_field_edges
        || limits.maximum_field_depth > maximum_implementation_field_depth
        || limits.maximum_owned_string_bytes > maximum_implementation_string_bytes
        || limits.maximum_enum_entries > maximum_implementation_enum_entries
        || limits.maximum_qbit_materialized_streams > maximum_implementation_qbit_materialized_streams)
        invalidTypeArgument("a caller limit exceeds the implementation maximum");
}

void chargeString(CanonicalTypeArgumentAdmissionStatistics & statistics, std::string_view value, const CanonicalTypeArgumentLimits & limits)
{
    addProspectively(
        statistics.owned_string_bytes,
        checkedSize(value.size(), "owned string length does not fit UInt64"),
        limits.maximum_owned_string_bytes,
        "owned string bytes");
}

void validateUnaliased(const ASTWithAlias & node)
{
    if (!node.alias.empty() || node.parametrised_alias || node.preferAliasToColumnName())
        invalidTypeArgument("aliases are not part of a data-type argument AST");
}

UInt64 qbitElementBits(const ASTPtr & element)
{
    const auto * data_type = element ? element->as<ASTDataType>() : nullptr;
    if (!data_type)
        return 0;
    const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
    if (!classification)
        return 0;
    const std::string_view canonical_name = classification.family->canonical_creator_name;
    if (canonical_name == "Int8")
        return 8;
    if (canonical_name == "BFloat16")
        return 16;
    if (canonical_name == "Float32")
        return 32;
    if (canonical_name == "Float64")
        return 64;
    return 0;
}

void chargeQBitMaterialization(
    const ASTDataType & data_type, CanonicalTypeArgumentAdmissionStatistics & statistics, const CanonicalTypeArgumentLimits & limits)
{
    const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type.name);
    if (!classification || classification.family->canonical_creator_name != "QBit")
        return;

    const auto * arguments = data_type.getArguments() ? data_type.getArguments()->as<ASTExpressionList>() : nullptr;
    if (!arguments || (arguments->children.size() != 2 && arguments->children.size() != 3))
        return;
    const UInt64 element_bits = qbitElementBits(arguments->children[0]);
    const ASTPtr & dimension_node = arguments->children[1];
    const ASTPtr & stride_node = arguments->children.size() == 3 ? arguments->children[2] : dimension_node;
    const auto * dimension_literal = dimension_node ? dimension_node->as<ASTLiteral>() : nullptr;
    const auto * stride_literal = stride_node ? stride_node->as<ASTLiteral>() : nullptr;
    if (element_bits == 0 || !dimension_literal || !stride_literal || dimension_literal->value.getType() != Field::Types::UInt64
        || stride_literal->value.getType() != Field::Types::UInt64)
        return;

    const UInt64 dimension = dimension_literal->value.safeGet<UInt64>();
    const UInt64 stride = stride_literal->value.safeGet<UInt64>();
    if (stride == 0 || stride > dimension || dimension % stride != 0 || (stride != dimension && stride % 8 != 0))
        return;
    const UInt64 groups = dimension / stride;
    if (groups > std::numeric_limits<UInt64>::max() / element_bits)
        typeArgumentLimit("QBit materialized streams");
    addProspectively(
        statistics.qbit_materialized_streams, groups * element_bits, limits.maximum_qbit_materialized_streams, "QBit materialized streams");
}

bool isParserNullsAction(NullsAction action) noexcept
{
    return action == NullsAction::EMPTY || action == NullsAction::RESPECT_NULLS || action == NullsAction::IGNORE_NULLS;
}

void validateFieldShape(
    const Field & field, CanonicalTypeArgumentAdmissionStatistics & statistics, UInt64 depth, const CanonicalTypeArgumentLimits & limits)
{
    if (depth > limits.maximum_field_depth)
        typeArgumentLimit("Field depth");
    addProspectively(statistics.field_node_occurrences, 1, limits.maximum_field_nodes, "Field node occurrences");
    statistics.maximum_field_depth = std::max(statistics.maximum_field_depth, depth);

    const auto validateChildren = [&](const auto & children)
    {
        addProspectively(
            statistics.field_edges,
            checkedSize(children.size(), "Field child count does not fit UInt64"),
            limits.maximum_field_edges,
            "Field edges");
        for (const auto & child : children)
            validateFieldShape(child, statistics, depth + 1, limits);
    };

    switch (field.getType())
    {
        case Field::Types::Null:
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64:
        case Field::Types::Bool:
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UInt256:
        case Field::Types::Int256:
        case Field::Types::Decimal32:
        case Field::Types::Decimal64:
        case Field::Types::Decimal128:
        case Field::Types::Decimal256:
        case Field::Types::UUID:
        case Field::Types::IPv4:
        case Field::Types::IPv6: return;
        case Field::Types::String: chargeString(statistics, field.safeGet<String>(), limits); return;
        case Field::Types::Array: validateChildren(field.safeGet<Array>()); return;
        case Field::Types::Tuple: validateChildren(field.safeGet<Tuple>()); return;
        case Field::Types::Map: {
            const auto & entries = field.safeGet<Map>();
            addProspectively(
                statistics.field_edges,
                checkedSize(entries.size(), "Map entry count does not fit UInt64"),
                limits.maximum_field_edges,
                "Field edges");
            for (const auto & entry : entries)
            {
                if (entry.getType() != Field::Types::Tuple || entry.safeGet<Tuple>().size() != 2)
                    invalidTypeArgument("Map literal has a malformed entry");
                validateFieldShape(entry, statistics, depth + 1, limits);
            }
            return;
        }
        case Field::Types::Object: {
            const auto & entries = field.safeGet<Object>();
            addProspectively(
                statistics.field_edges,
                checkedSize(entries.size(), "Object entry count does not fit UInt64"),
                limits.maximum_field_edges,
                "Field edges");
            for (const auto & [key, value] : entries)
            {
                chargeString(statistics, key, limits);
                validateFieldShape(value, statistics, depth + 1, limits);
            }
            return;
        }
        case Field::Types::AggregateFunctionState: {
            const auto & state = field.safeGet<AggregateFunctionStateData>();
            chargeString(statistics, state.name, limits);
            chargeString(statistics, state.data, limits);
            return;
        }
        case Field::Types::CustomType: invalidTypeArgument("CustomType literal is outside the frozen canonical Field surface");
    }
    invalidTypeArgument("literal Field kind is outside the frozen canonical Field surface");
}

void validateNodeShape(
    const IAST & node, CanonicalTypeArgumentAdmissionStatistics & statistics, UInt64 depth, const CanonicalTypeArgumentLimits & limits)
{
    addProspectively(statistics.ast_node_occurrences, 1, limits.maximum_ast_nodes, "AST node occurrences");
    addProspectively(
        statistics.ast_edges,
        checkedSize(node.children.size(), "AST child count does not fit UInt64"),
        limits.maximum_ast_edges,
        "AST edges");
    statistics.maximum_ast_depth = std::max(statistics.maximum_ast_depth, depth);
    if (depth > limits.maximum_ast_depth)
        typeArgumentLimit("AST depth");

    if (const auto * enum_type = node.as<ASTEnumDataType>())
    {
        if (!enum_type->children.empty() || !BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(enum_type->name))
            invalidTypeArgument("specialized Enum node has an invalid shape or family");
        chargeString(statistics, enum_type->name, limits);
        addProspectively(
            statistics.enum_entries,
            checkedSize(enum_type->values.size(), "Enum entry count does not fit UInt64"),
            limits.maximum_enum_entries,
            "Enum entries");
        for (const auto & [name, value] : enum_type->values)
        {
            static_cast<void>(value);
            chargeString(statistics, name, limits);
        }
        return;
    }

    if (const auto * tuple = node.as<ASTTupleDataType>())
    {
        if (!BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(tuple->name) || tuple->children.size() > 1
            || (!tuple->children.empty() && !tuple->children.front()->as<ASTExpressionList>()))
            invalidTypeArgument("specialized Tuple node has an invalid shape or family");
        const std::size_t argument_count = tuple->children.empty() ? 0 : tuple->children.front()->as<ASTExpressionList>()->children.size();
        if (!tuple->element_names.empty() && tuple->element_names.size() != argument_count)
            invalidTypeArgument("specialized Tuple labels do not match its arguments");
        chargeString(statistics, tuple->name, limits);
        for (const auto & name : tuple->element_names)
            chargeString(statistics, name, limits);
        return;
    }

    if (const auto * data_type = node.as<ASTDataType>())
    {
        const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
        if (!classification || data_type->children.size() > 1
            || (!data_type->children.empty() && !data_type->children.front()->as<ASTExpressionList>()))
            invalidTypeArgument("generic data-type node is unknown or malformed");
        chargeString(statistics, data_type->name, limits);
        chargeQBitMaterialization(*data_type, statistics, limits);
        if (classification.input_class == BuiltInDataTypeCreatorInputClass::CanonicalizeGenericEnumArguments)
        {
            const auto * arguments = data_type->getArguments() ? data_type->getArguments()->as<ASTExpressionList>() : nullptr;
            if (!arguments)
                invalidTypeArgument("generic Enum has no argument list");
            addProspectively(
                statistics.enum_entries,
                checkedSize(arguments->children.size(), "generic Enum entry count does not fit UInt64"),
                limits.maximum_enum_entries,
                "Enum entries");
        }
        return;
    }

    if (const auto * list = node.as<ASTExpressionList>())
    {
        if (list->getSeparator() != ',')
            invalidTypeArgument("data-type argument list has a noncanonical separator");
        return;
    }

    if (const auto * pair = node.as<ASTNameTypePair>())
    {
        if (!pair->type || pair->children.size() != 1 || pair->children.front().get() != pair->type.get())
            invalidTypeArgument("name/type pair has inconsistent ownership");
        chargeString(statistics, pair->name, limits);
        return;
    }

    if (const auto * object_path = node.as<ASTObjectTypedPathArgument>())
    {
        if (!object_path->type || object_path->children.size() != 1 || object_path->children.front().get() != object_path->type.get())
            invalidTypeArgument("JSON typed-path argument has inconsistent ownership");
        chargeString(statistics, object_path->path, limits);
        return;
    }

    if (const auto * object_argument = node.as<ASTObjectTypeArgument>())
    {
        const std::array<const ASTPtr *, 4> variants{
            &object_argument->path_with_type, &object_argument->skip_path, &object_argument->skip_path_regexp, &object_argument->parameter};
        const auto active = std::ranges::count_if(variants, [](const ASTPtr * value) { return static_cast<bool>(*value); });
        if (active != 1 || object_argument->children.size() != 1)
            invalidTypeArgument("JSON argument must contain exactly one owned variant");
        const auto owner = std::ranges::find_if(variants, [](const ASTPtr * value) { return static_cast<bool>(*value); });
        if (owner == variants.end() || object_argument->children.front().get() != (**owner).get())
            invalidTypeArgument("JSON argument variant disagrees with its child owner");
        return;
    }

    if (const auto * function = node.as<ASTFunction>())
    {
        validateUnaliased(*function);
        if (function->parameters || function->window_definition || !function->window_name.empty() || function->isWindowFunction()
            || function->computeAfterWindowFunctions() || function->isLambdaFunction() || function->preferSubqueryToFunctionFormatting()
            || function->noEmptyArgs() || function->isCompoundName() || function->getKind() != ASTFunction::Kind::ORDINARY_FUNCTION
            || !isParserNullsAction(function->getNullsAction())
            || function->children.size() != (function->arguments ? std::size_t{1} : std::size_t{0})
            || (function->arguments && function->children.front().get() != function->arguments.get()))
            invalidTypeArgument("function-shaped data-type argument has unsupported state");
        chargeString(statistics, function->name, limits);
        return;
    }

    if (const auto * identifier = node.as<ASTIdentifier>())
    {
        validateUnaliased(*identifier);
        const auto semantic_string_bytes = identifier->getParserIdentifierSemanticStringBytes();
        if (!semantic_string_bytes)
            invalidTypeArgument("identifier argument has unsupported or empty state");
        addProspectively(
            statistics.ast_edges,
            checkedSize(identifier->name_parts.size(), "identifier part count does not fit UInt64"),
            limits.maximum_ast_edges,
            "AST owned entries");
        chargeString(statistics, identifier->full_name, limits);
        for (const auto & part : identifier->name_parts)
            chargeString(statistics, part, limits);
        addProspectively(statistics.owned_string_bytes, *semantic_string_bytes, limits.maximum_owned_string_bytes, "owned string bytes");
        return;
    }

    if (const auto * literal = node.as<ASTLiteral>())
    {
        validateUnaliased(*literal);
        if (!literal->children.empty() || !literal->unique_column_name.empty() || literal->getUseLegacyColumnNameOfTuple())
            invalidTypeArgument("literal argument has non-syntax state");
        validateFieldShape(literal->value, statistics, 1, limits);
        return;
    }

    invalidTypeArgument("AST node kind is outside the ParserDataType surface");
}

CanonicalTypeArgumentAdmissionStatistics preflightTypeArgumentAST(const ASTPtr & root, const CanonicalTypeArgumentLimits & limits)
{
    if (!root || (!root->as<ASTDataType>() && !root->as<ASTTupleDataType>() && !root->as<ASTEnumDataType>()))
        invalidTypeArgument("root must be a non-null data-type AST");

    struct Frame
    {
        const IAST * node = nullptr;
        std::size_t next_child = 0;
    };
    std::array<Frame, maximum_implementation_ast_depth> stack{};
    std::size_t stack_size = 1;
    stack.front().node = root.get();
    CanonicalTypeArgumentAdmissionStatistics statistics;
    validateNodeShape(*root, statistics, 1, limits);

    while (stack_size != 0)
    {
        Frame & frame = stack[stack_size - 1];
        if (frame.next_child == frame.node->children.size())
        {
            --stack_size;
            continue;
        }
        const ASTPtr & child = frame.node->children[frame.next_child++];
        if (!child)
            invalidTypeArgument("AST contains a null child");
        if (stack_size >= limits.maximum_ast_depth)
            typeArgumentLimit("AST depth");
        for (std::size_t ancestor = 0; ancestor < stack_size; ++ancestor)
            if (stack[ancestor].node == child.get())
                invalidTypeArgument("AST contains a cycle");
        stack[stack_size++] = {.node = child.get(), .next_child = 0};
        validateNodeShape(*child, statistics, checkedSize(stack_size, "AST depth does not fit UInt64"), limits);
    }
    return statistics;
}

std::pair<String, Int64> readExplicitEnumEntry(const ASTPtr & entry)
{
    const auto * function = entry ? entry->as<ASTFunction>() : nullptr;
    if (!function || function->name != "equals" || function->parameters || !function->arguments
        || function->arguments->children.size() != 2)
        invalidTypeArgument("explicit generic Enum entry has an invalid shape");
    const auto * name = function->arguments->children[0]->as<ASTLiteral>();
    const auto * value = function->arguments->children[1]->as<ASTLiteral>();
    if (!name || !value || name->value.getType() != Field::Types::String
        || (value->value.getType() != Field::Types::UInt64 && value->value.getType() != Field::Types::Int64))
        invalidTypeArgument("explicit generic Enum entry has invalid literals");
    Int64 signed_value = 0;
    if (value->value.getType() == Field::Types::UInt64)
    {
        const UInt64 unsigned_value = value->value.safeGet<UInt64>();
        if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
            invalidTypeArgument("explicit generic Enum value exceeds Int64");
        signed_value = static_cast<Int64>(unsigned_value);
    }
    else
        signed_value = value->value.safeGet<Int64>();
    return {name->value.safeGet<String>(), signed_value};
}

void rebindOwnedChildren(ASTPtr & node)
{
    if (auto * pair = node->as<ASTNameTypePair>())
        pair->type = pair->children.front();
    else if (auto * object_path = node->as<ASTObjectTypedPathArgument>())
        object_path->type = object_path->children.front();
    else if (auto * object_argument = node->as<ASTObjectTypeArgument>())
    {
        if (object_argument->path_with_type)
            object_argument->path_with_type = object_argument->children.front();
        else if (object_argument->skip_path)
            object_argument->skip_path = object_argument->children.front();
        else if (object_argument->skip_path_regexp)
            object_argument->skip_path_regexp = object_argument->children.front();
        else if (object_argument->parameter)
            object_argument->parameter = object_argument->children.front();
    }
    else if (auto * function = node->as<ASTFunction>(); function && function->arguments)
        function->arguments = function->children.front();
}

const String & readImplicitEnumEntry(const ASTPtr & entry)
{
    const auto * literal = entry ? entry->as<ASTLiteral>() : nullptr;
    if (!literal || literal->value.getType() != Field::Types::String)
        invalidTypeArgument("implicit generic Enum entry is not a string literal");
    return literal->value.safeGet<String>();
}

void canonicalizeMutableGenericEnums(ASTPtr & node, CanonicalTypeArgumentAdmissionStatistics & statistics)
{
    for (auto & child : node->children)
        canonicalizeMutableGenericEnums(child, statistics);
    rebindOwnedChildren(node);

    const auto * data_type = node->as<ASTDataType>();
    if (!data_type || node->as<ASTEnumDataType>())
        return;
    const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
    if (!classification || classification.input_class != BuiltInDataTypeCreatorInputClass::CanonicalizeGenericEnumArguments)
        return;
    const ASTPtr arguments = data_type->getArguments();
    const auto * list = arguments ? arguments->as<ASTExpressionList>() : nullptr;
    if (!list || list->children.empty())
        invalidTypeArgument("factory accepted a generic Enum without arguments");

    auto canonical = make_intrusive<ASTEnumDataType>();
    canonical->name = String(classification.family->canonical_creator_name);
    canonical->values.reserve(list->children.size());

    Int64 implicit_base = 1;
    UInt64 implicit_offset = 0;
    bool is_first = true;
    for (const auto & entry : list->children)
    {
        if (entry && entry->as<ASTLiteral>())
        {
            if (!is_first)
                ++implicit_offset;
            if (implicit_offset > static_cast<UInt64>(std::numeric_limits<Int64>::max())
                || implicit_base > std::numeric_limits<Int64>::max() - static_cast<Int64>(implicit_offset))
                invalidTypeArgument("auto-assigned generic Enum value exceeds Int64");
            canonical->values.emplace_back(readImplicitEnumEntry(entry), implicit_base + static_cast<Int64>(implicit_offset));
        }
        else
        {
            auto explicit_entry = readExplicitEnumEntry(entry);
            if (is_first)
                implicit_base = explicit_entry.second;
            canonical->values.push_back(std::move(explicit_entry));
        }
        is_first = false;
    }
    if (implicit_offset != 0 && implicit_offset != list->children.size() - 1)
        invalidTypeArgument("generic Enum mixes explicit values inside an implicit suffix");

    node = std::move(canonical);
    ++statistics.generic_enums_canonicalized;
}

void appendUInt16LE(String & output, UInt16 value)
{
    output.push_back(static_cast<char>(value));
    output.push_back(static_cast<char>(value >> 8));
}

void appendUInt64LE(String & output, UInt64 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendUInt32LE(String & output, UInt32 value)
{
    for (size_t index = 0; index < sizeof(value); ++index)
        output.push_back(static_cast<char>(value >> (8 * index)));
}

void appendVarUInt(String & output, UInt64 value)
{
    while (value >= 0x80)
    {
        output.push_back(static_cast<char>(static_cast<UInt8>(value) | 0x80));
        value >>= 7;
    }
    output.push_back(static_cast<char>(value));
}

UInt64 varUIntSize(UInt64 value) noexcept
{
    UInt64 result = 1;
    while (value >= 0x80)
    {
        value >>= 7;
        ++result;
    }
    return result;
}

UInt64 framedSize(std::string_view value)
{
    const UInt64 size = checkedSize(value.size(), "canonical argument frame length does not fit UInt64");
    if (size > std::numeric_limits<UInt64>::max() - varUIntSize(size))
        typeArgumentLimit("canonical argument frame bytes");
    return varUIntSize(size) + size;
}

void appendFrame(String & output, std::string_view value)
{
    if (!std::in_range<UInt64>(value.size()))
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument is too large for UInt64 framing");
    appendVarUInt(output, static_cast<UInt64>(value.size()));
    output.append(value);
}

void checkUnsignedRange(ParameterKind kind, UInt64 value)
{
    const UInt64 maximum = kind == ParameterKind::UInt8 ? std::numeric_limits<UInt8>::max()
        : kind == ParameterKind::UInt16                 ? std::numeric_limits<UInt16>::max()
        : kind == ParameterKind::UInt32                 ? std::numeric_limits<UInt32>::max()
                                                        : std::numeric_limits<UInt64>::max();
    if (!isUnsignedIntegerParameter(kind) || value > maximum)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Unsigned user-defined type argument is outside its declared kind");
}

void checkSignedRange(ParameterKind kind, Int64 value)
{
    const Int64 minimum = kind == ParameterKind::Int8 ? std::numeric_limits<Int8>::min()
        : kind == ParameterKind::Int16                ? std::numeric_limits<Int16>::min()
        : kind == ParameterKind::Int32                ? std::numeric_limits<Int32>::min()
                                                      : std::numeric_limits<Int64>::min();
    const Int64 maximum = kind == ParameterKind::Int8 ? std::numeric_limits<Int8>::max()
        : kind == ParameterKind::Int16                ? std::numeric_limits<Int16>::max()
        : kind == ParameterKind::Int32                ? std::numeric_limits<Int32>::max()
                                                      : std::numeric_limits<Int64>::max();
    if (!isSignedIntegerParameter(kind) || value < minimum || value > maximum)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Signed user-defined type argument is outside its declared kind");
}

UInt64 canonicalItemSize(const CanonicalTypeArgumentValue & actual)
{
    const UInt8 kind_tag = static_cast<UInt8>(actual.kind);
    if (kind_tag < static_cast<UInt8>(ParameterKind::Type) || kind_tag > static_cast<UInt8>(ParameterKind::String))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Unknown user-defined type argument kind");

    UInt64 payload_size = 0;
    switch (actual.kind)
    {
        case ParameterKind::Type:
            if (!std::holds_alternative<CanonicalTypeArgument>(actual.value))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "TYPE argument has the wrong representation");
            payload_size = framedSize(std::get<CanonicalTypeArgument>(actual.value).getBinaryEncoding());
            break;
        case ParameterKind::Bool:
            if (!std::holds_alternative<bool>(actual.value))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Bool argument has the wrong representation");
            payload_size = 1;
            break;
        case ParameterKind::UInt8:
        case ParameterKind::UInt16:
        case ParameterKind::UInt32:
        case ParameterKind::UInt64:
            if (!std::holds_alternative<UInt64>(actual.value))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Unsigned argument has the wrong representation");
            checkUnsignedRange(actual.kind, std::get<UInt64>(actual.value));
            payload_size = UInt64{1} << (static_cast<UInt8>(actual.kind) - static_cast<UInt8>(ParameterKind::UInt8));
            break;
        case ParameterKind::Int8:
        case ParameterKind::Int16:
        case ParameterKind::Int32:
        case ParameterKind::Int64:
            if (!std::holds_alternative<Int64>(actual.value))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "Signed argument has the wrong representation");
            checkSignedRange(actual.kind, std::get<Int64>(actual.value));
            payload_size = UInt64{1} << (static_cast<UInt8>(actual.kind) - static_cast<UInt8>(ParameterKind::Int8));
            break;
        case ParameterKind::String:
            if (!std::holds_alternative<String>(actual.value))
                throw Exception(ErrorCodes::TYPE_MISMATCH, "String argument has the wrong representation");
            payload_size = framedSize(std::get<String>(actual.value));
            break;
    }
    if (payload_size == std::numeric_limits<UInt64>::max())
        typeArgumentLimit("canonical argument item bytes");
    return payload_size + 1;
}

void appendCanonicalItem(String & output, const CanonicalTypeArgumentValue & actual)
{
    output.push_back(static_cast<char>(actual.kind));
    switch (actual.kind)
    {
        case ParameterKind::Type: appendFrame(output, std::get<CanonicalTypeArgument>(actual.value).getBinaryEncoding()); break;
        case ParameterKind::Bool: output.push_back(std::get<bool>(actual.value) ? 1 : 0); break;
        case ParameterKind::UInt8: output.push_back(static_cast<char>(std::get<UInt64>(actual.value))); break;
        case ParameterKind::UInt16: appendUInt16LE(output, static_cast<UInt16>(std::get<UInt64>(actual.value))); break;
        case ParameterKind::UInt32: appendUInt32LE(output, static_cast<UInt32>(std::get<UInt64>(actual.value))); break;
        case ParameterKind::UInt64: appendUInt64LE(output, std::get<UInt64>(actual.value)); break;
        case ParameterKind::Int8: output.push_back(static_cast<char>(static_cast<Int8>(std::get<Int64>(actual.value)))); break;
        case ParameterKind::Int16: appendUInt16LE(output, static_cast<UInt16>(static_cast<Int16>(std::get<Int64>(actual.value)))); break;
        case ParameterKind::Int32: appendUInt32LE(output, static_cast<UInt32>(static_cast<Int32>(std::get<Int64>(actual.value)))); break;
        case ParameterKind::Int64: appendUInt64LE(output, static_cast<UInt64>(std::get<Int64>(actual.value))); break;
        case ParameterKind::String: appendFrame(output, std::get<String>(actual.value)); break;
    }
}

class CanonicalArgumentsReader final
{
public:
    explicit CanonicalArgumentsReader(std::string_view input_)
        : input(input_)
    {
    }

    UInt8 readByte()
    {
        if (position == input.size())
            invalidCanonicalArguments("truncated input");
        return static_cast<UInt8>(input[position++]);
    }

    UInt16 readUInt16LE()
    {
        UInt16 result = readByte();
        result |= static_cast<UInt16>(readByte()) << 8;
        return result;
    }

    UInt64 readLittleEndian(size_t width)
    {
        UInt64 result = 0;
        for (size_t index = 0; index < width; ++index)
            result |= static_cast<UInt64>(readByte()) << (8 * index);
        return result;
    }

    UInt64 readMinimalVarUInt()
    {
        UInt64 result = 0;
        for (size_t index = 0; index < 10; ++index)
        {
            const UInt8 byte = readByte();
            if (index == 9 && byte > 1)
                invalidCanonicalArguments("VarUInt overflows UInt64");
            result |= static_cast<UInt64>(byte & 0x7f) << (7 * index);
            if ((byte & 0x80) == 0)
            {
                if (index != 0 && byte == 0)
                    invalidCanonicalArguments("VarUInt is not minimally encoded");
                return result;
            }
        }
        invalidCanonicalArguments("VarUInt overflows UInt64");
    }

    std::string_view readFrame()
    {
        const UInt64 length = readMinimalVarUInt();
        if (!std::in_range<size_t>(length))
            typeArgumentLimit("frame length exceeds the host size domain");
        return readBytes(static_cast<size_t>(length));
    }

    std::string_view readBytes(size_t length)
    {
        if (length > input.size() - position)
            invalidCanonicalArguments("truncated input");
        const std::string_view result = input.substr(position, length);
        position += length;
        return result;
    }

    size_t offset() const noexcept { return position; }
    size_t remaining() const noexcept { return input.size() - position; }
    bool eof() const noexcept { return position == input.size(); }

private:
    std::string_view input;
    size_t position = 0;
};

enum class CanonicalFieldTag : UInt8
{
    Null = 0x00,
    UInt64 = 0x01,
    Int64 = 0x02,
    UInt128 = 0x03,
    Int128 = 0x04,
    UInt256 = 0x05,
    Int256 = 0x06,
    Float64 = 0x07,
    Decimal32 = 0x08,
    Decimal64 = 0x09,
    Decimal128 = 0x0A,
    Decimal256 = 0x0B,
    String = 0x0C,
    Array = 0x0D,
    Tuple = 0x0E,
    Map = 0x0F,
    IPv4 = 0x10,
    IPv6 = 0x11,
    UUID = 0x12,
    Bool = 0x13,
    Object = 0x14,
    AggregateFunctionState = 0x15,
    NegativeInfinity = 0xFE,
    PositiveInfinity = 0xFF,
};

class CanonicalTypeBinaryDecoder final
{
public:
    CanonicalTypeBinaryDecoder(std::string_view input, const CanonicalTypeArgumentLimits & limits_)
        : reader(input)
        , limits(limits_)
    {
    }

    ASTPtr decode()
    {
        ASTPtr result = decodeType(1);
        if (!reader.eof())
            invalidCanonicalArguments("TYPE frame has trailing bytes");
        return result;
    }

private:
    struct StructuredAST
    {
        ASTPtr root;
        ASTExpressionList * arguments = nullptr;
    };

    void chargeASTNode(UInt64 depth, UInt64 edges)
    {
        if (depth > limits.maximum_ast_depth)
            typeArgumentLimit("AST depth");
        addProspectively(statistics.ast_node_occurrences, 1, limits.maximum_ast_nodes, "AST node occurrences");
        addProspectively(statistics.ast_edges, edges, limits.maximum_ast_edges, "AST edges");
        statistics.maximum_ast_depth = std::max(statistics.maximum_ast_depth, depth);
    }

    void chargeFieldNode(UInt64 depth, UInt64 edges)
    {
        if (depth > limits.maximum_field_depth)
            typeArgumentLimit("Field depth");
        addProspectively(statistics.field_node_occurrences, 1, limits.maximum_field_nodes, "Field node occurrences");
        addProspectively(statistics.field_edges, edges, limits.maximum_field_edges, "Field edges");
        statistics.maximum_field_depth = std::max(statistics.maximum_field_depth, depth);
    }

    size_t checkedContainerSize(UInt64 size, std::string_view description) const
    {
        if (!std::in_range<size_t>(size))
            typeArgumentLimit(description);
        return static_cast<size_t>(size);
    }

    void chargeIdentifier(std::string_view name)
    {
        addProspectively(statistics.ast_edges, 1, limits.maximum_ast_edges, "AST owned entries");
        chargeString(statistics, name, limits);
        chargeString(statistics, name, limits);
    }

    StructuredAST startStructuredType(std::string_view name, UInt64 argument_count, UInt64 depth, bool force_arguments = false)
    {
        const bool has_arguments = force_arguments || argument_count != 0;
        chargeASTNode(depth, has_arguments ? 1 : 0);
        chargeString(statistics, name, limits);
        const size_t host_count = checkedContainerSize(argument_count, "AST argument count exceeds the host size domain");

        auto root = make_intrusive<ASTDataType>();
        root->name = String(name);
        if (!has_arguments)
            return {.root = std::move(root), .arguments = nullptr};

        chargeASTNode(depth + 1, argument_count);
        auto arguments = make_intrusive<ASTExpressionList>();
        arguments->children.reserve(host_count);
        ASTExpressionList * arguments_ptr = arguments.get();
        root->children.push_back(std::move(arguments));
        return {.root = std::move(root), .arguments = arguments_ptr};
    }

    void reserveAdditionalArguments(ASTExpressionList & arguments, UInt64 count)
    {
        addProspectively(statistics.ast_edges, count, limits.maximum_ast_edges, "AST edges");
        const size_t host_count = checkedContainerSize(count, "AST argument count exceeds the host size domain");
        if (host_count > std::numeric_limits<size_t>::max() - arguments.children.size())
            typeArgumentLimit("AST argument count exceeds the host size domain");
        arguments.children.reserve(arguments.children.size() + host_count);
    }

    ASTPtr makeSimpleType(std::string_view name, UInt64 depth) { return startStructuredType(name, 0, depth).root; }

    ASTPtr makeUnsignedLiteral(UInt64 value, UInt64 depth)
    {
        chargeFieldNode(1, 0);
        chargeASTNode(depth, 0);
        return make_intrusive<ASTLiteral>(Field(value));
    }

    ASTPtr makeStringLiteral(std::string_view value, UInt64 depth)
    {
        chargeFieldNode(1, 0);
        chargeString(statistics, value, limits);
        chargeASTNode(depth, 0);
        return make_intrusive<ASTLiteral>(Field(String(value)));
    }

    ASTPtr makeDecodedFieldLiteral(Field value, UInt64 depth)
    {
        chargeASTNode(depth, 0);
        return make_intrusive<ASTLiteral>(std::move(value));
    }

    ASTPtr makeIdentifier(std::string_view name, UInt64 depth)
    {
        chargeASTNode(depth, 0);
        chargeIdentifier(name);
        return make_intrusive<ASTIdentifier>(String(name));
    }

    StructuredAST startFunction(std::string_view name, UInt64 argument_count, UInt64 depth)
    {
        chargeASTNode(depth, 1);
        chargeString(statistics, name, limits);
        chargeASTNode(depth + 1, argument_count);
        const size_t host_count = checkedContainerSize(argument_count, "function argument count exceeds the host size domain");

        auto root = make_intrusive<ASTFunction>();
        root->name = String(name);
        root->arguments = make_intrusive<ASTExpressionList>();
        root->arguments->children.reserve(host_count);
        ASTExpressionList * arguments_ptr = root->arguments->as<ASTExpressionList>();
        root->children.push_back(root->arguments);
        return {.root = std::move(root), .arguments = arguments_ptr};
    }

    ASTPtr makeEquals(std::string_view name, UInt64 value, UInt64 depth)
    {
        auto function = startFunction("equals", 2, depth);
        function.arguments->children.push_back(makeIdentifier(name, depth + 2));
        function.arguments->children.push_back(makeUnsignedLiteral(value, depth + 2));
        return std::move(function.root);
    }

    ASTPtr makeObjectSetting(std::string_view name, UInt64 value, UInt64 depth)
    {
        chargeASTNode(depth, 1);
        auto wrapper = make_intrusive<ASTObjectTypeArgument>();
        wrapper->parameter = makeEquals(name, value, depth + 1);
        wrapper->children.push_back(wrapper->parameter);
        return wrapper;
    }

    ASTPtr makeObjectTypedPath(std::string_view path, UInt64 depth)
    {
        chargeASTNode(depth, 1);
        auto wrapper = make_intrusive<ASTObjectTypeArgument>();

        chargeASTNode(depth + 1, 1);
        chargeString(statistics, path, limits);
        auto typed_path = make_intrusive<ASTObjectTypedPathArgument>();
        typed_path->path = String(path);
        typed_path->type = decodeChildType(depth + 2);
        typed_path->children.push_back(typed_path->type);
        wrapper->path_with_type = std::move(typed_path);
        wrapper->children.push_back(wrapper->path_with_type);
        return wrapper;
    }

    ASTPtr makeObjectSkipPath(std::string_view path, UInt64 depth)
    {
        chargeASTNode(depth, 1);
        auto wrapper = make_intrusive<ASTObjectTypeArgument>();
        wrapper->skip_path = makeIdentifier(path, depth + 1);
        wrapper->children.push_back(wrapper->skip_path);
        return wrapper;
    }

    ASTPtr makeObjectSkipRegexp(std::string_view regexp, UInt64 depth)
    {
        chargeASTNode(depth, 1);
        auto wrapper = make_intrusive<ASTObjectTypeArgument>();
        wrapper->skip_path_regexp = makeStringLiteral(regexp, depth + 1);
        wrapper->children.push_back(wrapper->skip_path_regexp);
        return wrapper;
    }

    template <typename T>
    T readLittleEndian()
    {
        ReadBufferFromString input(reader.readBytes(sizeof(T)));
        T result{};
        readBinaryLittleEndian(result, input);
        return result;
    }

    template <typename Decimal>
    Field decodeDecimalField()
    {
        const UInt64 scale = reader.readMinimalVarUInt();
        if (scale > std::numeric_limits<UInt32>::max())
            invalidCanonicalArguments("decimal Field scale exceeds UInt32");
        return DecimalField<Decimal>(readLittleEndian<Decimal>(), static_cast<UInt32>(scale));
    }

    Field decodeChildField(UInt64 depth)
    {
        if (depth > limits.maximum_field_depth)
            typeArgumentLimit("Field depth");
        return decodeField(depth);
    }

    Field decodeField(UInt64 depth)
    {
        const auto tag = static_cast<CanonicalFieldTag>(reader.readByte());
        chargeFieldNode(depth, 0);
        switch (tag)
        {
            case CanonicalFieldTag::Null: return Null();
            case CanonicalFieldTag::PositiveInfinity: return POSITIVE_INFINITY;
            case CanonicalFieldTag::NegativeInfinity: return NEGATIVE_INFINITY;
            case CanonicalFieldTag::UInt64: return reader.readMinimalVarUInt();
            case CanonicalFieldTag::Int64: {
                const UInt64 raw = reader.readMinimalVarUInt();
                return static_cast<Int64>((raw >> 1) ^ (0 - (raw & 1)));
            }
            case CanonicalFieldTag::UInt128: return readLittleEndian<UInt128>();
            case CanonicalFieldTag::Int128: return readLittleEndian<Int128>();
            case CanonicalFieldTag::UInt256: return readLittleEndian<UInt256>();
            case CanonicalFieldTag::Int256: return readLittleEndian<Int256>();
            case CanonicalFieldTag::Float64: return readLittleEndian<Float64>();
            case CanonicalFieldTag::Decimal32: return decodeDecimalField<Decimal32>();
            case CanonicalFieldTag::Decimal64: return decodeDecimalField<Decimal64>();
            case CanonicalFieldTag::Decimal128: return decodeDecimalField<Decimal128>();
            case CanonicalFieldTag::Decimal256: return decodeDecimalField<Decimal256>();
            case CanonicalFieldTag::String: {
                const std::string_view value = reader.readFrame();
                chargeString(statistics, value, limits);
                return String(value);
            }
            case CanonicalFieldTag::UUID: return readLittleEndian<UUID>();
            case CanonicalFieldTag::IPv4: return readLittleEndian<IPv4>();
            case CanonicalFieldTag::IPv6: return readLittleEndian<IPv6>();
            case CanonicalFieldTag::Bool: {
                const UInt8 value = reader.readByte();
                if (value > 1)
                    invalidCanonicalArguments("Bool Field payload must be zero or one");
                return value != 0;
            }
            case CanonicalFieldTag::Array:
            case CanonicalFieldTag::Tuple: {
                const UInt64 count = reader.readMinimalVarUInt();
                addProspectively(statistics.field_edges, count, limits.maximum_field_edges, "Field edges");
                const size_t host_count = checkedContainerSize(count, "Field child count exceeds the host size domain");
                if (tag == CanonicalFieldTag::Array)
                {
                    Array value;
                    value.reserve(host_count);
                    for (size_t index = 0; index < host_count; ++index)
                        value.push_back(decodeChildField(depth + 1));
                    return value;
                }
                Tuple value;
                value.reserve(host_count);
                for (size_t index = 0; index < host_count; ++index)
                    value.push_back(decodeChildField(depth + 1));
                return value;
            }
            case CanonicalFieldTag::Map: {
                const UInt64 count = reader.readMinimalVarUInt();
                addProspectively(statistics.field_edges, count, limits.maximum_field_edges, "Field edges");
                const size_t host_count = checkedContainerSize(count, "Map entry count exceeds the host size domain");
                Map value;
                value.reserve(host_count);
                for (size_t index = 0; index < host_count; ++index)
                {
                    chargeFieldNode(depth + 1, 2);
                    Tuple entry;
                    entry.reserve(2);
                    entry.push_back(decodeChildField(depth + 2));
                    entry.push_back(decodeChildField(depth + 2));
                    value.emplace_back(std::move(entry));
                }
                return value;
            }
            case CanonicalFieldTag::Object: {
                const UInt64 count = reader.readMinimalVarUInt();
                addProspectively(statistics.field_edges, count, limits.maximum_field_edges, "Field edges");
                const size_t host_count = checkedContainerSize(count, "Object entry count exceeds the host size domain");
                Object value;
                for (size_t index = 0; index < host_count; ++index)
                {
                    const std::string_view key = reader.readFrame();
                    chargeString(statistics, key, limits);
                    Field child = decodeChildField(depth + 1);
                    value.emplace(String(key), std::move(child));
                }
                return value;
            }
            case CanonicalFieldTag::AggregateFunctionState: {
                const std::string_view name = reader.readFrame();
                chargeString(statistics, name, limits);
                const std::string_view data = reader.readFrame();
                chargeString(statistics, data, limits);
                return AggregateFunctionStateData{.name = String(name), .data = String(data)};
            }
        }
        invalidCanonicalArguments("unknown Field tag in TYPE frame");
    }

    template <typename Value>
    ASTPtr decodeEnum(std::string_view name, UInt64 depth)
    {
        const UInt64 count = reader.readMinimalVarUInt();
        chargeASTNode(depth, 0);
        chargeString(statistics, name, limits);
        addProspectively(statistics.enum_entries, count, limits.maximum_enum_entries, "Enum entries");
        const size_t host_count = checkedContainerSize(count, "Enum entry count exceeds the host size domain");

        auto result = make_intrusive<ASTEnumDataType>();
        result->name = String(name);
        result->values.reserve(host_count);
        for (size_t index = 0; index < host_count; ++index)
        {
            const std::string_view entry_name = reader.readFrame();
            chargeString(statistics, entry_name, limits);
            result->values.emplace_back(String(entry_name), static_cast<Int64>(readLittleEndian<Value>()));
        }
        return result;
    }

    ASTPtr decodeTuple(bool named, UInt64 depth)
    {
        const UInt64 count = reader.readMinimalVarUInt();
        const size_t host_count = checkedContainerSize(count, "Tuple element count exceeds the host size domain");
        chargeASTNode(depth, 1);
        chargeString(statistics, "Tuple", limits);
        chargeASTNode(depth + 1, count);

        auto result = make_intrusive<ASTTupleDataType>();
        result->name = "Tuple";
        auto arguments = make_intrusive<ASTExpressionList>();
        arguments->children.reserve(host_count);
        if (named)
            result->element_names.reserve(host_count);
        for (size_t index = 0; index < host_count; ++index)
        {
            if (named)
            {
                const std::string_view element_name = reader.readFrame();
                chargeString(statistics, element_name, limits);
                result->element_names.emplace_back(element_name);
            }
            arguments->children.push_back(decodeChildType(depth + 2));
        }
        result->children.push_back(std::move(arguments));
        return result;
    }

    ASTPtr decodeNested(UInt64 depth)
    {
        const UInt64 count = reader.readMinimalVarUInt();
        auto result = startStructuredType("Nested", count, depth, true);
        const size_t host_count = checkedContainerSize(count, "Nested element count exceeds the host size domain");
        for (size_t index = 0; index < host_count; ++index)
        {
            const std::string_view name = reader.readFrame();
            chargeASTNode(depth + 2, 1);
            chargeString(statistics, name, limits);
            auto pair = make_intrusive<ASTNameTypePair>();
            pair->name = String(name);
            pair->type = decodeChildType(depth + 3);
            pair->children.push_back(pair->type);
            result.arguments->children.push_back(std::move(pair));
        }
        return std::move(result.root);
    }

    ASTPtr decodeAggregateFunction(bool has_version, UInt64 depth)
    {
        UInt64 version = 0;
        if (has_version)
            version = reader.readMinimalVarUInt();
        const std::string_view function_name = reader.readFrame();
        const UInt64 parameter_count = reader.readMinimalVarUInt();
        const size_t host_parameter_count = checkedContainerSize(parameter_count, "aggregate parameter count exceeds the host size domain");

        ASTPtr function_specifier;
        if (parameter_count == 0)
        {
            function_specifier = makeIdentifier(function_name, depth + 2);
        }
        else
        {
            auto function = startFunction(function_name, parameter_count, depth + 2);
            for (size_t index = 0; index < host_parameter_count; ++index)
                function.arguments->children.push_back(makeDecodedFieldLiteral(decodeField(1), depth + 4));
            function_specifier = std::move(function.root);
        }

        const UInt64 type_count = reader.readMinimalVarUInt();
        if (type_count > std::numeric_limits<UInt64>::max() - 1 - static_cast<UInt64>(has_version))
            typeArgumentLimit("aggregate type argument count");
        const UInt64 root_argument_count = type_count + 1 + static_cast<UInt64>(has_version);
        auto result = startStructuredType(has_version ? "AggregateFunction" : "SimpleAggregateFunction", root_argument_count, depth, true);
        if (has_version)
            result.arguments->children.push_back(makeUnsignedLiteral(version, depth + 2));
        result.arguments->children.push_back(std::move(function_specifier));
        const size_t host_type_count = checkedContainerSize(type_count, "aggregate type argument count exceeds the host size domain");
        for (size_t index = 0; index < host_type_count; ++index)
            result.arguments->children.push_back(decodeChildType(depth + 2));
        return std::move(result.root);
    }

    ASTPtr decodeJSON(UInt64 depth)
    {
        if (reader.readByte() != 0)
            invalidCanonicalArguments("unsupported JSON type encoding version");
        const UInt64 maximum_dynamic_paths = reader.readMinimalVarUInt();
        const UInt64 maximum_dynamic_types = reader.readByte();
        auto result = startStructuredType("JSON", 0, depth, true);
        reserveAdditionalArguments(*result.arguments, 2);
        result.arguments->children.push_back(makeObjectSetting("max_dynamic_paths", maximum_dynamic_paths, depth + 2));
        result.arguments->children.push_back(makeObjectSetting("max_dynamic_types", maximum_dynamic_types, depth + 2));

        const UInt64 typed_path_count = reader.readMinimalVarUInt();
        reserveAdditionalArguments(*result.arguments, typed_path_count);
        const size_t host_typed_path_count = checkedContainerSize(typed_path_count, "JSON typed-path count exceeds the host size domain");
        for (size_t index = 0; index < host_typed_path_count; ++index)
        {
            const std::string_view path = reader.readFrame();
            result.arguments->children.push_back(makeObjectTypedPath(path, depth + 2));
        }

        const UInt64 skip_path_count = reader.readMinimalVarUInt();
        reserveAdditionalArguments(*result.arguments, skip_path_count);
        const size_t host_skip_path_count = checkedContainerSize(skip_path_count, "JSON skip-path count exceeds the host size domain");
        for (size_t index = 0; index < host_skip_path_count; ++index)
        {
            const std::string_view path = reader.readFrame();
            result.arguments->children.push_back(makeObjectSkipPath(path, depth + 2));
        }

        const UInt64 skip_regexp_count = reader.readMinimalVarUInt();
        reserveAdditionalArguments(*result.arguments, skip_regexp_count);
        const size_t host_skip_regexp_count
            = checkedContainerSize(skip_regexp_count, "JSON skip-regexp count exceeds the host size domain");
        for (size_t index = 0; index < host_skip_regexp_count; ++index)
        {
            const std::string_view regexp = reader.readFrame();
            result.arguments->children.push_back(makeObjectSkipRegexp(regexp, depth + 2));
        }
        return std::move(result.root);
    }

    static std::string_view intervalTypeName(UInt8 kind)
    {
        switch (kind)
        {
            case 0: return "IntervalNanosecond";
            case 1: return "IntervalMicrosecond";
            case 2: return "IntervalMillisecond";
            case 3: return "IntervalSecond";
            case 4: return "IntervalMinute";
            case 5: return "IntervalHour";
            case 6: return "IntervalDay";
            case 7: return "IntervalWeek";
            case 8: return "IntervalMonth";
            case 9: return "IntervalQuarter";
            case 10: return "IntervalYear";
            default: invalidCanonicalArguments("unknown Interval kind in TYPE frame");
        }
    }

    ASTPtr decodeChildType(UInt64 depth)
    {
        if (depth > limits.maximum_ast_depth)
            typeArgumentLimit("AST depth");
        return decodeType(depth);
    }

    ASTPtr decodeType(UInt64 depth)
    {
        const auto index = static_cast<BinaryTypeIndex>(reader.readByte());
        switch (index)
        {
            case BinaryTypeIndex::Nothing: return makeSimpleType("Nothing", depth);
            case BinaryTypeIndex::UInt8: return makeSimpleType("UInt8", depth);
            case BinaryTypeIndex::UInt16: return makeSimpleType("UInt16", depth);
            case BinaryTypeIndex::UInt32: return makeSimpleType("UInt32", depth);
            case BinaryTypeIndex::UInt64: return makeSimpleType("UInt64", depth);
            case BinaryTypeIndex::UInt128: return makeSimpleType("UInt128", depth);
            case BinaryTypeIndex::UInt256: return makeSimpleType("UInt256", depth);
            case BinaryTypeIndex::Int8: return makeSimpleType("Int8", depth);
            case BinaryTypeIndex::Int16: return makeSimpleType("Int16", depth);
            case BinaryTypeIndex::Int32: return makeSimpleType("Int32", depth);
            case BinaryTypeIndex::Int64: return makeSimpleType("Int64", depth);
            case BinaryTypeIndex::Int128: return makeSimpleType("Int128", depth);
            case BinaryTypeIndex::Int256: return makeSimpleType("Int256", depth);
            case BinaryTypeIndex::BFloat16: return makeSimpleType("BFloat16", depth);
            case BinaryTypeIndex::Float32: return makeSimpleType("Float32", depth);
            case BinaryTypeIndex::Float64: return makeSimpleType("Float64", depth);
            case BinaryTypeIndex::Date: return makeSimpleType("Date", depth);
            case BinaryTypeIndex::Date32: return makeSimpleType("Date32", depth);
            case BinaryTypeIndex::DateTimeUTC: return makeSimpleType("DateTime", depth);
            case BinaryTypeIndex::DateTimeWithTimezone: {
                auto result = startStructuredType("DateTime", 1, depth);
                result.arguments->children.push_back(makeStringLiteral(reader.readFrame(), depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::DateTime64UTC: {
                auto result = startStructuredType("DateTime64", 1, depth);
                result.arguments->children.push_back(makeUnsignedLiteral(reader.readByte(), depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::DateTime64WithTimezone: {
                const UInt64 scale = reader.readByte();
                const std::string_view timezone = reader.readFrame();
                auto result = startStructuredType("DateTime64", 2, depth);
                result.arguments->children.push_back(makeUnsignedLiteral(scale, depth + 2));
                result.arguments->children.push_back(makeStringLiteral(timezone, depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Time: return makeSimpleType("Time", depth);
            case BinaryTypeIndex::Time64: {
                auto result = startStructuredType("Time64", 1, depth);
                result.arguments->children.push_back(makeUnsignedLiteral(reader.readByte(), depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::String: return makeSimpleType("String", depth);
            case BinaryTypeIndex::FixedString: {
                auto result = startStructuredType("FixedString", 1, depth);
                result.arguments->children.push_back(makeUnsignedLiteral(reader.readMinimalVarUInt(), depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Enum8: return decodeEnum<Int8>("Enum8", depth);
            case BinaryTypeIndex::Enum16: return decodeEnum<Int16>("Enum16", depth);
            case BinaryTypeIndex::Decimal32:
            case BinaryTypeIndex::Decimal64:
            case BinaryTypeIndex::Decimal128:
            case BinaryTypeIndex::Decimal256: {
                const UInt64 precision = reader.readByte();
                const UInt64 scale = reader.readByte();
                auto result = startStructuredType("Decimal", 2, depth);
                result.arguments->children.push_back(makeUnsignedLiteral(precision, depth + 2));
                result.arguments->children.push_back(makeUnsignedLiteral(scale, depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::UUID: return makeSimpleType("UUID", depth);
            case BinaryTypeIndex::Array: {
                auto result = startStructuredType("Array", 1, depth);
                result.arguments->children.push_back(decodeChildType(depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::UnnamedTuple: return decodeTuple(false, depth);
            case BinaryTypeIndex::NamedTuple: return decodeTuple(true, depth);
            case BinaryTypeIndex::Set: invalidCanonicalArguments("Set has no canonical parser-surface inverse");
            case BinaryTypeIndex::Interval: return makeSimpleType(intervalTypeName(reader.readByte()), depth);
            case BinaryTypeIndex::Nullable: {
                auto result = startStructuredType("Nullable", 1, depth);
                result.arguments->children.push_back(decodeChildType(depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Function: invalidCanonicalArguments("Function has no canonical parser-surface inverse");
            case BinaryTypeIndex::AggregateFunction: return decodeAggregateFunction(true, depth);
            case BinaryTypeIndex::LowCardinality: {
                auto result = startStructuredType("LowCardinality", 1, depth);
                result.arguments->children.push_back(decodeChildType(depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Map: {
                auto result = startStructuredType("Map", 2, depth);
                result.arguments->children.push_back(decodeChildType(depth + 2));
                result.arguments->children.push_back(decodeChildType(depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::IPv4: return makeSimpleType("IPv4", depth);
            case BinaryTypeIndex::IPv6: return makeSimpleType("IPv6", depth);
            case BinaryTypeIndex::Variant: {
                const UInt64 count = reader.readMinimalVarUInt();
                auto result = startStructuredType("Variant", count, depth, true);
                const size_t host_count = checkedContainerSize(count, "Variant count exceeds the host size domain");
                for (size_t child = 0; child < host_count; ++child)
                    result.arguments->children.push_back(decodeChildType(depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Dynamic: {
                auto result = startStructuredType("Dynamic", 1, depth);
                result.arguments->children.push_back(makeEquals("max_types", reader.readByte(), depth + 2));
                return std::move(result.root);
            }
            case BinaryTypeIndex::Custom: {
                const std::string_view name = reader.readFrame();
                if (!isSupportedCanonicalCustomName(name))
                    invalidCanonicalArguments("Custom name is outside the canonical TYPE-argument surface");
                return makeSimpleType(name, depth);
            }
            case BinaryTypeIndex::Bool: return makeSimpleType("Bool", depth);
            case BinaryTypeIndex::SimpleAggregateFunction: return decodeAggregateFunction(false, depth);
            case BinaryTypeIndex::Nested: return decodeNested(depth);
            case BinaryTypeIndex::JSON: return decodeJSON(depth);
            case BinaryTypeIndex::QBit:
            case BinaryTypeIndex::QBitWithStride: {
                const bool has_stride = index == BinaryTypeIndex::QBitWithStride;
                auto result = startStructuredType("QBit", has_stride ? 3 : 2, depth);
                result.arguments->children.push_back(decodeChildType(depth + 2));
                result.arguments->children.push_back(makeUnsignedLiteral(reader.readMinimalVarUInt(), depth + 2));
                if (has_stride)
                    result.arguments->children.push_back(makeUnsignedLiteral(reader.readMinimalVarUInt(), depth + 2));
                return std::move(result.root);
            }
        }
        invalidCanonicalArguments("unknown type tag in TYPE frame");
    }

    CanonicalArgumentsReader reader;
    const CanonicalTypeArgumentLimits & limits;
    CanonicalTypeArgumentAdmissionStatistics statistics;
};

ASTPtr decodeCanonicalTypeArgumentAST(std::string_view encoded, const CanonicalTypeArgumentLimits & limits)
{
    CanonicalTypeBinaryDecoder decoder(encoded, limits);
    ASTPtr result = decoder.decode();
    static_cast<void>(preflightTypeArgumentAST(result, limits));
    return result;
}

Int64 decodeSignedInteger(UInt64 raw, ParameterKind kind)
{
    switch (kind)
    {
        case ParameterKind::Int8: return std::bit_cast<Int8>(static_cast<UInt8>(raw));
        case ParameterKind::Int16: return std::bit_cast<Int16>(static_cast<UInt16>(raw));
        case ParameterKind::Int32: return std::bit_cast<Int32>(static_cast<UInt32>(raw));
        case ParameterKind::Int64: return std::bit_cast<Int64>(raw);
        default: throw Exception(ErrorCodes::TYPE_MISMATCH, "Canonical signed argument has a non-signed kind");
    }
}

size_t integerWidth(ParameterKind kind)
{
    if (isUnsignedIntegerParameter(kind))
        return size_t{1} << (static_cast<UInt8>(kind) - static_cast<UInt8>(ParameterKind::UInt8));
    if (isSignedIntegerParameter(kind))
        return size_t{1} << (static_cast<UInt8>(kind) - static_cast<UInt8>(ParameterKind::Int8));
    throw Exception(ErrorCodes::TYPE_MISMATCH, "Canonical integer argument has a non-integer kind");
}
}

void validateCanonicalTypeArgumentLimits(const CanonicalTypeArgumentLimits & limits)
{
    validateTypeArgumentLimits(limits);
}

CanonicalTypeArgument CanonicalTypeArgument::fromFactoryValidatedAST(
    const ASTPtr & type_ast, const CanonicalTypeArgumentLimits & limits, CanonicalTypeArgumentAdmissionStatistics * output_statistics)
{
    validateTypeArgumentLimits(limits);
    auto statistics = preflightTypeArgumentAST(type_ast, limits);
    ASTPtr owned_ast = type_ast->clone();
    canonicalizeMutableGenericEnums(owned_ast, statistics);
    DataTypePtr type = DataTypeFactory::instance().get(owned_ast);
    statistics.factory_calls = 1;
    String binary_encoding = encodeCanonicalDataType(type);
    ASTPtr canonical_ast = decodeCanonicalTypeArgumentAST(binary_encoding, limits);
    CanonicalTypeArgument result(std::move(canonical_ast), type, type->getName(), std::move(binary_encoding));
    if (output_statistics)
        *output_statistics = statistics;
    return result;
}

CanonicalTypeArgumentValue CanonicalTypeArgumentValue::type(const ASTPtr & type_ast, const CanonicalTypeArgumentLimits & limits)
{
    return {.kind = ParameterKind::Type, .value = CanonicalTypeArgument::fromFactoryValidatedAST(type_ast, limits)};
}

CanonicalTypeArgumentValue CanonicalTypeArgumentValue::boolean(bool boolean_value)
{
    return {.kind = ParameterKind::Bool, .value = boolean_value};
}

CanonicalTypeArgumentValue CanonicalTypeArgumentValue::unsignedInteger(ParameterKind parameter_kind, UInt64 integer_value)
{
    checkUnsignedRange(parameter_kind, integer_value);
    return {.kind = parameter_kind, .value = integer_value};
}

CanonicalTypeArgumentValue CanonicalTypeArgumentValue::signedInteger(ParameterKind parameter_kind, Int64 integer_value)
{
    checkSignedRange(parameter_kind, integer_value);
    return {.kind = parameter_kind, .value = integer_value};
}

CanonicalTypeArgumentValue CanonicalTypeArgumentValue::string(String string_value)
{
    return {.kind = ParameterKind::String, .value = std::move(string_value)};
}

CanonicalTypeArguments CanonicalTypeArguments::validate(
    std::span<const Parameter> parameters,
    std::vector<CanonicalTypeArgumentValue> actuals,
    UInt64 maximum_total_bytes,
    UInt64 maximum_item_bytes)
{
    if (actuals.size() != parameters.size())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "User-defined type argument count does not match its declaration");
    if (actuals.size() > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "User-defined type argument count exceeds the canonical wire domain");

    UInt64 encoded_size = sizeof(UInt16) + varUIntSize(static_cast<UInt64>(actuals.size()));
    if (encoded_size > maximum_total_bytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument byte limit exceeded");
    for (size_t index = 0; index < actuals.size(); ++index)
    {
        const auto & actual = actuals[index];
        if (actual.kind != parameters[index].kind)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "User-defined type argument kind does not match formal {}", index);
        const UInt64 item_size = canonicalItemSize(actual);
        if (item_size > maximum_item_bytes || item_size > maximum_total_bytes - encoded_size)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument byte limit exceeded");
        encoded_size += item_size;
    }

    String encoded;
    if (!std::in_range<std::size_t>(encoded_size) || encoded_size > encoded.max_size())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument exceeds the host string domain");
    encoded.reserve(static_cast<std::size_t>(encoded_size));
    appendUInt16LE(encoded, 1);
    appendVarUInt(encoded, static_cast<UInt64>(actuals.size()));
    for (const auto & actual : actuals)
        appendCanonicalItem(encoded, actual);
    if (encoded.size() != encoded_size)
        std::terminate();
    return CanonicalTypeArguments(std::move(actuals), std::move(encoded));
}

CanonicalTypeArguments CanonicalTypeArguments::decode(
    std::span<const Parameter> parameters,
    std::string_view encoded,
    UInt64 maximum_total_bytes,
    UInt64 maximum_item_bytes,
    const CanonicalTypeArgumentLimits & type_limits)
{
    validateTypeArgumentLimits(type_limits);
    if (!std::in_range<UInt64>(encoded.size()) || static_cast<UInt64>(encoded.size()) > maximum_total_bytes)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument byte limit exceeded");
    if (parameters.size() > std::numeric_limits<UInt16>::max())
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "User-defined type argument count exceeds the canonical wire domain");

    CanonicalArgumentsReader reader(encoded);
    if (reader.readUInt16LE() != 1)
        invalidCanonicalArguments("unsupported format version");
    const UInt64 count = reader.readMinimalVarUInt();
    if (count != parameters.size())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "User-defined type argument count does not match its declaration");
    if (count > reader.remaining() / 2)
        invalidCanonicalArguments("argument count exceeds the remaining canonical bytes");

    std::vector<CanonicalTypeArgumentValue> actuals;
    actuals.reserve(parameters.size());
    for (size_t index = 0; index < parameters.size(); ++index)
    {
        const size_t item_start = reader.offset();
        const UInt8 kind_tag = reader.readByte();
        if (kind_tag < static_cast<UInt8>(ParameterKind::Type) || kind_tag > static_cast<UInt8>(ParameterKind::String))
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Unknown user-defined type argument kind");
        const auto kind = static_cast<ParameterKind>(kind_tag);
        if (kind != parameters[index].kind)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "User-defined type argument kind does not match formal {}", index);

        switch (kind)
        {
            case ParameterKind::Type: {
                const std::string_view type_encoding = reader.readFrame();
                if (reader.offset() - item_start > maximum_item_bytes)
                    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument item byte limit exceeded");

                ASTPtr canonical_ast = decodeCanonicalTypeArgumentAST(type_encoding, type_limits);
                DataTypePtr physical_type = DataTypeFactory::instance().get(canonical_ast);
                String canonical_encoding = encodeCanonicalDataType(physical_type);
                if (canonical_encoding != type_encoding)
                    invalidCanonicalArguments("TYPE structure does not reproduce its canonical encoding");
                String canonical_name = physical_type->getName();

                CanonicalTypeArgument type_argument(
                    std::move(canonical_ast), std::move(physical_type), std::move(canonical_name), std::move(canonical_encoding));
                actuals.push_back({.kind = kind, .value = std::move(type_argument)});
                break;
            }
            case ParameterKind::Bool: {
                const UInt8 value = reader.readByte();
                if (value > 1)
                    invalidCanonicalArguments("Bool payload must be zero or one");
                actuals.push_back(CanonicalTypeArgumentValue::boolean(value != 0));
                break;
            }
            case ParameterKind::UInt8:
            case ParameterKind::UInt16:
            case ParameterKind::UInt32:
            case ParameterKind::UInt64:
                actuals.push_back(CanonicalTypeArgumentValue::unsignedInteger(kind, reader.readLittleEndian(integerWidth(kind))));
                break;
            case ParameterKind::Int8:
            case ParameterKind::Int16:
            case ParameterKind::Int32:
            case ParameterKind::Int64:
                actuals.push_back(
                    CanonicalTypeArgumentValue::signedInteger(
                        kind, decodeSignedInteger(reader.readLittleEndian(integerWidth(kind)), kind)));
                break;
            case ParameterKind::String: {
                const std::string_view value = reader.readFrame();
                if (reader.offset() - item_start > maximum_item_bytes)
                    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument item byte limit exceeded");
                actuals.push_back(CanonicalTypeArgumentValue::string(String(value)));
                break;
            }
        }

        if (reader.offset() - item_start > maximum_item_bytes)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Canonical user-defined type argument item byte limit exceeded");
    }
    if (!reader.eof())
        invalidCanonicalArguments("trailing bytes");

    auto result = validate(parameters, std::move(actuals), maximum_total_bytes, maximum_item_bytes);
    if (result.encoded() != encoded)
        invalidCanonicalArguments("input does not match its canonical re-encoding");
    return result;
}
}
