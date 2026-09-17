#include <DataTypes/UDT/DefinitionLowering.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>

#include <Core/Field.h>

#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ASTUDTTemplate.h>
#include <Parsers/parseFieldFromCastedLiteral.h>

#include <Common/Exception.h>

#include <algorithm>
#include <array>
#include <limits>
#include <numeric>
#include <optional>
#include <ranges>
#include <unordered_set>
#include <utility>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LIMIT_EXCEEDED;
}

namespace DB::UDT
{
namespace
{

constexpr UInt64 maximum_implementation_definitions = 100'000;
constexpr UInt64 maximum_implementation_formals = std::numeric_limits<UInt16>::max();
constexpr UInt64 maximum_implementation_ast_nodes = 1ULL << 20;
constexpr UInt64 maximum_implementation_ast_edges = 4ULL << 20;
constexpr UInt64 maximum_implementation_ast_depth = 128;
constexpr UInt64 maximum_implementation_output_nodes = 1ULL << 20;
constexpr UInt64 maximum_implementation_output_edges = 4ULL << 20;
constexpr UInt64 maximum_implementation_enum_entries = 1ULL << 20;
constexpr UInt64 maximum_implementation_string_bytes = 64ULL << 20;
constexpr UInt64 maximum_implementation_catalog_string_bytes = 1ULL << 30;
constexpr UInt64 maximum_implementation_dependencies = std::numeric_limits<UInt16>::max();
constexpr UInt64 maximum_implementation_field_nodes = 1ULL << 20;
constexpr UInt64 maximum_implementation_field_edges = 4ULL << 20;
constexpr UInt64 maximum_implementation_field_entries = 1ULL << 20;
constexpr UInt64 maximum_implementation_field_depth = 256;
constexpr UInt64 maximum_implementation_field_literal_bytes = 64ULL << 20;

[[noreturn]] void invalid(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid user-defined type definition AST: {}", message);
}

[[noreturn]] void limitExceeded(std::string_view message)
{
    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "User-defined type definition lowering limit exceeded: {}", message);
}

UInt64 checkedSize(std::size_t value, std::string_view what)
{
    if (!std::in_range<UInt64>(value))
        limitExceeded(what);
    return static_cast<UInt64>(value);
}

void addProspectively(UInt64 & current, UInt64 amount, UInt64 maximum, std::string_view what)
{
    if (amount > maximum || current > maximum - amount)
        limitExceeded(what);
    current += amount;
}

bool containsZero(std::string_view value) noexcept
{
    return value.find('\0') != std::string_view::npos;
}

bool isLowerHexDigest(std::string_view value) noexcept
{
    if (value.size() != 64)
        return false;
    return std::ranges::all_of(
        value, [](unsigned char character) { return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'); });
}

void validateLimits(const DefinitionLoweringLimits & limits)
{
    const auto & field = limits.field_values;
    if (limits.maximum_definitions == 0 || limits.maximum_formals == 0 || limits.maximum_ast_nodes == 0 || limits.maximum_ast_edges == 0
        || limits.maximum_ast_depth == 0 || limits.maximum_output_nodes == 0 || limits.maximum_output_edges == 0
        || limits.maximum_enum_entries == 0 || limits.maximum_string_bytes == 0 || limits.maximum_catalog_string_bytes == 0
        || limits.maximum_total_string_bytes == 0 || limits.maximum_dependencies == 0 || field.maximum_nodes == 0
        || field.maximum_edges == 0 || field.maximum_entries == 0 || field.maximum_depth == 0 || field.maximum_literal_bytes == 0)
        invalid("every lowering limit must be nonzero");

    if (limits.maximum_definitions > maximum_implementation_definitions || limits.maximum_formals > maximum_implementation_formals
        || limits.maximum_ast_nodes > maximum_implementation_ast_nodes || limits.maximum_ast_edges > maximum_implementation_ast_edges
        || limits.maximum_ast_depth > maximum_implementation_ast_depth || limits.maximum_output_nodes > maximum_implementation_output_nodes
        || limits.maximum_output_edges > maximum_implementation_output_edges
        || limits.maximum_enum_entries > maximum_implementation_enum_entries
        || limits.maximum_string_bytes > maximum_implementation_string_bytes
        || limits.maximum_catalog_string_bytes > maximum_implementation_catalog_string_bytes
        || limits.maximum_total_string_bytes > maximum_implementation_string_bytes
        || limits.maximum_dependencies > maximum_implementation_dependencies || field.maximum_nodes > maximum_implementation_field_nodes
        || field.maximum_edges > maximum_implementation_field_edges || field.maximum_entries > maximum_implementation_field_entries
        || field.maximum_depth > maximum_implementation_field_depth
        || field.maximum_literal_bytes > maximum_implementation_field_literal_bytes)
        invalid("a caller lowering limit exceeds the implementation maximum");
}

class StringBudget
{
public:
    explicit StringBudget(const DefinitionLoweringLimits & limits_)
        : maximum_string_bytes(limits_.maximum_string_bytes)
        , maximum_total_string_bytes(limits_.maximum_total_string_bytes)
    {
    }

    StringBudget(UInt64 maximum_string_bytes_, UInt64 maximum_total_string_bytes_)
        : maximum_string_bytes(maximum_string_bytes_)
        , maximum_total_string_bytes(maximum_total_string_bytes_)
    {
    }

    void charge(std::string_view value)
    {
        const UInt64 size = checkedSize(value.size(), "owned string length does not fit UInt64");
        chargeBytes(size);
    }

    void chargeBytes(UInt64 size)
    {
        if (size > maximum_string_bytes)
            limitExceeded("single owned string bytes");
        addProspectively(total, size, maximum_total_string_bytes, "total owned string bytes");
    }

    UInt64 remaining() const noexcept { return maximum_total_string_bytes - total; }
    UInt64 chargedBytes() const noexcept { return total; }

private:
    const UInt64 maximum_string_bytes;
    const UInt64 maximum_total_string_bytes;
    UInt64 total = 0;
};

void validateStructuredName(const StructuredDefinitionName & name, StringBudget & strings, std::string_view what)
{
    if (name.normalized_database_name.empty() || name.normalized_qualified_name.empty() || name.normalized_local_name.empty()
        || containsZero(name.normalized_database_name) || containsZero(name.normalized_qualified_name)
        || containsZero(name.normalized_local_name))
        invalid(what);
    if (name.normalized_database_name.size() > std::numeric_limits<std::size_t>::max() - 1
        || name.normalized_local_name.size() > std::numeric_limits<std::size_t>::max() - name.normalized_database_name.size() - 1)
        invalid(what);
    const std::size_t database_size = name.normalized_database_name.size();
    const std::size_t expected_qualified_size = database_size + 1 + name.normalized_local_name.size();
    if (name.normalized_qualified_name.size() != expected_qualified_size
        || !name.normalized_qualified_name.starts_with(name.normalized_database_name)
        || name.normalized_qualified_name[database_size] != '.'
        || std::string_view(name.normalized_qualified_name).substr(database_size + 1) != name.normalized_local_name)
        invalid(what);
    strings.charge(name.normalized_database_name);
    strings.charge(name.normalized_qualified_name);
    strings.charge(name.normalized_local_name);
}

void validateUnaliased(const ASTWithAlias & node)
{
    if (!node.alias.empty() || node.parametrised_alias || node.preferAliasToColumnName())
        invalid("aliases are outside the CREATE TYPE definition surface");
}

bool isParserNullsAction(NullsAction action) noexcept
{
    return action == NullsAction::EMPTY || action == NullsAction::RESPECT_NULLS || action == NullsAction::IGNORE_NULLS;
}

std::optional<std::string_view> canonicalEnumFamily(std::string_view name) noexcept
{
    const auto equalsAsciiCaseInsensitive = [](std::string_view lhs, std::string_view rhs)
    {
        if (lhs.size() != rhs.size())
            return false;
        for (std::size_t index = 0; index < lhs.size(); ++index)
        {
            const auto lower = [](unsigned char value)
            { return value >= static_cast<unsigned char>('A') && value <= static_cast<unsigned char>('Z') ? value + ('a' - 'A') : value; };
            if (lower(static_cast<unsigned char>(lhs[index])) != lower(static_cast<unsigned char>(rhs[index])))
                return false;
        }
        return true;
    };
    if (!equalsAsciiCaseInsensitive(name, "Enum") && !equalsAsciiCaseInsensitive(name, "Enum8")
        && !equalsAsciiCaseInsensitive(name, "Enum16"))
        return std::nullopt;
    const auto classification = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(name);
    if (!classification || !classification.family)
        return std::nullopt;
    const std::string_view canonical = classification.family->canonical_creator_name;
    if (canonical != "Enum" && canonical != "Enum8" && canonical != "Enum16")
        return std::nullopt;
    return canonical;
}

void validateOwnedChild(const IAST & owner, const ASTPtr & child, std::string_view what)
{
    if (!child || owner.children.size() != 1 || owner.children.front().get() != child.get())
        invalid(what);
}

void validateCreateChildren(const ASTCreateTypeQuery & query)
{
    std::array<const ASTPtr *, 6> possible{
        &query.database, &query.type_name, &query.parameters, &query.decreases, &query.definition, &query.comment};
    std::size_t expected = 0;
    for (const ASTPtr * child : possible)
    {
        if (!*child)
            continue;
        if (expected >= query.children.size() || query.children[expected].get() != child->get())
            invalid("CREATE TYPE fields disagree with their canonical child order");
        ++expected;
    }
    if (expected != query.children.size())
        invalid("CREATE TYPE has hidden or duplicate children");
}

void chargeLiteralString(const ASTLiteral & literal, StringBudget & strings)
{
    if (literal.value.getType() == Field::Types::String)
        strings.charge(literal.value.safeGet<String>());
}

struct FieldPreflight
{
    UInt64 nodes = 0;
    UInt64 edges = 0;
    UInt64 entries = 0;
    UInt64 literal_bytes = 0;
};

void preflightLiteralField(const Field & field, UInt64 depth, const CanonicalFieldValueLimits & limits, FieldPreflight & counters)
{
    if (depth > limits.maximum_depth)
        limitExceeded("literal Field depth");
    addProspectively(counters.nodes, 1, limits.maximum_nodes, "literal Field nodes");
    const auto chargeLiteral = [&](std::size_t size)
    {
        addProspectively(
            counters.literal_bytes,
            checkedSize(size, "literal Field payload length does not fit UInt64"),
            limits.maximum_literal_bytes,
            "literal Field bytes");
    };
    const auto chargeChildren = [&](std::size_t edges, std::size_t entries)
    {
        addProspectively(
            counters.edges,
            checkedSize(edges, "literal Field edge count does not fit UInt64"),
            limits.maximum_edges,
            "literal Field edges");
        addProspectively(
            counters.entries,
            checkedSize(entries, "literal Field entry count does not fit UInt64"),
            limits.maximum_entries,
            "literal Field entries");
    };

    switch (field.getType())
    {
        case Field::Types::Null: return;
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64: chargeLiteral(sizeof(UInt64)); return;
        case Field::Types::String: chargeLiteral(field.safeGet<String>().size()); return;
        case Field::Types::Bool: chargeLiteral(1); return;
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UUID:
        case Field::Types::IPv6: chargeLiteral(16); return;
        case Field::Types::UInt256:
        case Field::Types::Int256: chargeLiteral(32); return;
        case Field::Types::Decimal32: chargeLiteral(8); return;
        case Field::Types::Decimal64: chargeLiteral(12); return;
        case Field::Types::Decimal128: chargeLiteral(20); return;
        case Field::Types::Decimal256: chargeLiteral(36); return;
        case Field::Types::IPv4: chargeLiteral(4); return;
        case Field::Types::Array: {
            const auto & children = field.safeGet<Array>();
            chargeChildren(children.size(), children.size());
            for (const auto & child : children)
                preflightLiteralField(child, depth + 1, limits, counters);
            return;
        }
        case Field::Types::Tuple: {
            const auto & children = field.safeGet<Tuple>();
            chargeChildren(children.size(), children.size());
            for (const auto & child : children)
                preflightLiteralField(child, depth + 1, limits, counters);
            return;
        }
        case Field::Types::Map: {
            const auto & entries = field.safeGet<Map>();
            if (entries.size() > std::numeric_limits<std::size_t>::max() / 2)
                limitExceeded("literal Map edges");
            chargeChildren(entries.size() * 2, entries.size());
            for (const auto & entry : entries)
            {
                if (entry.getType() != Field::Types::Tuple || entry.safeGet<Tuple>().size() != 2)
                    invalid("literal Map contains a malformed entry");
                const auto & pair = entry.safeGet<Tuple>();
                preflightLiteralField(pair[0], depth + 1, limits, counters);
                preflightLiteralField(pair[1], depth + 1, limits, counters);
            }
            return;
        }
        case Field::Types::Object: {
            const auto & entries = field.safeGet<Object>();
            chargeChildren(entries.size(), entries.size());
            for (const auto & [key, child] : entries)
            {
                chargeLiteral(key.size());
                preflightLiteralField(child, depth + 1, limits, counters);
            }
            return;
        }
        case Field::Types::AggregateFunctionState: {
            const auto & state = field.safeGet<AggregateFunctionStateData>();
            chargeLiteral(state.name.size());
            chargeLiteral(state.data.size());
            return;
        }
        case Field::Types::CustomType: invalid("CustomType literal is outside the canonical Field surface");
    }
    invalid("literal Field kind is outside the canonical surface");
}

void validateASTNodeSurface(
    const IAST & node, const DefinitionLoweringLimits & limits, StringBudget & strings, FieldPreflight & field_counters)
{
    if (const auto * query = node.as<ASTCreateTypeQuery>())
    {
        validateCreateChildren(*query);
        strings.charge(query->cluster);
        if (query->definition_hash)
            strings.charge(*query->definition_hash);
        return;
    }
    if (const auto * declaration = node.as<ASTUDTParameterDeclaration>())
    {
        if (!declaration->children.empty())
            invalid("formal declaration has children");
        strings.charge(declaration->name);
        return;
    }
    if (const auto * reference = node.as<ASTUDTTypeParameterReference>())
    {
        if (!reference->children.empty())
            invalid("TYPE formal reference has children");
        strings.charge(reference->name);
        return;
    }
    if (const auto * reference = node.as<ASTUDTValueParameterReference>())
    {
        if (!reference->children.empty())
            invalid("value formal reference has children");
        strings.charge(reference->name);
        return;
    }
    if (const auto * predicate = node.as<ASTUDTIsZero>())
    {
        validateOwnedChild(*predicate, predicate->parameter_reference, "TYPE_IF predicate has inconsistent ownership");
        return;
    }
    if (const auto * decrement = node.as<ASTUDTDecrement>())
    {
        validateOwnedChild(*decrement, decrement->parameter_reference, "decrement has inconsistent ownership");
        return;
    }
    if (const auto * type_if = node.as<ASTUDTTypeIf>())
    {
        if (!type_if->predicate || !type_if->then_type || !type_if->else_type || type_if->children.size() != 3
            || type_if->children[0].get() != type_if->predicate.get() || type_if->children[1].get() != type_if->then_type.get()
            || type_if->children[2].get() != type_if->else_type.get())
            invalid("TYPE_IF has inconsistent ownership");
        return;
    }
    if (const auto * reference = node.as<ASTUDTReference>())
    {
        if (reference->children.size() > 1 || (!reference->children.empty() && !reference->children.front()->as<ASTExpressionList>()))
            invalid("qualified definition reference has an invalid argument owner");
        if (!reference->children.empty() && reference->children.front()->children.empty())
            invalid("qualified definition reference retains an empty argument owner");
        strings.charge(reference->database_name);
        strings.charge(reference->type_name);
        return;
    }
    if (const auto * enumeration = node.as<ASTEnumDataType>())
    {
        if (!enumeration->children.empty() || enumeration->values.empty() || !canonicalEnumFamily(enumeration->name))
            invalid("specialized Enum has an invalid shape or family");
        strings.charge(enumeration->name);
        for (const auto & [name, value] : enumeration->values)
        {
            static_cast<void>(value);
            strings.charge(name);
        }
        return;
    }
    if (const auto * tuple = node.as<ASTTupleDataType>())
    {
        if (tuple->name != "Tuple" || tuple->children.size() != 1 || !tuple->children.front()->as<ASTExpressionList>()
            || tuple->children.front()->children.empty())
            invalid("specialized Tuple has an invalid shape or family");
        const std::size_t arguments = tuple->children.empty() ? 0 : tuple->children.front()->as<ASTExpressionList>()->children.size();
        if (!tuple->element_names.empty() && tuple->element_names.size() != arguments)
            invalid("specialized Tuple labels do not match its arguments");
        strings.charge(tuple->name);
        for (const auto & label : tuple->element_names)
            strings.charge(label);
        return;
    }
    if (const auto * data_type = node.as<ASTDataType>())
    {
        if (data_type->children.size() > 1 || (!data_type->children.empty() && !data_type->children.front()->as<ASTExpressionList>()))
            invalid("generic data type has an invalid argument owner");
        if (!data_type->children.empty() && data_type->children.front()->children.empty())
            invalid("generic data type retains an empty argument owner");
        strings.charge(data_type->name);
        return;
    }
    if (const auto * list = node.as<ASTExpressionList>())
    {
        if (list->getSeparator() != ',')
            invalid("expression list has a noncanonical separator");
        return;
    }
    if (const auto * pair = node.as<ASTNameTypePair>())
    {
        validateOwnedChild(*pair, pair->type, "name/type pair has inconsistent ownership");
        strings.charge(pair->name);
        return;
    }
    if (const auto * path = node.as<ASTObjectTypedPathArgument>())
    {
        validateOwnedChild(*path, path->type, "JSON typed path has inconsistent ownership");
        strings.charge(path->path);
        return;
    }
    if (const auto * argument = node.as<ASTObjectTypeArgument>())
    {
        const std::array<const ASTPtr *, 4> variants{
            &argument->path_with_type, &argument->skip_path, &argument->skip_path_regexp, &argument->parameter};
        const ASTPtr * owner = nullptr;
        std::size_t active = 0;
        for (const ASTPtr * variant : variants)
        {
            if (!*variant)
                continue;
            owner = variant;
            ++active;
        }
        if (active != 1 || !owner || argument->children.size() != 1 || argument->children.front().get() != owner->get())
            invalid("JSON argument must own exactly one active variant");
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
            invalid("function-shaped argument has unsupported state");
        strings.charge(function->name);
        return;
    }
    if (const auto * identifier = node.as<ASTIdentifier>())
    {
        validateUnaliased(*identifier);
        const auto semantic_bytes = identifier->getParserIdentifierSemanticStringBytes();
        if (!semantic_bytes)
            invalid("identifier has unsupported parser state");
        strings.charge(identifier->full_name);
        for (const auto & part : identifier->name_parts)
            strings.charge(part);
        strings.chargeBytes(checkedSize(*semantic_bytes, "identifier semantic bytes do not fit UInt64"));
        return;
    }
    if (const auto * literal = node.as<ASTLiteral>())
    {
        validateUnaliased(*literal);
        if (!literal->children.empty() || !literal->unique_column_name.empty() || literal->getUseLegacyColumnNameOfTuple())
            invalid("literal has non-syntax state");
        preflightLiteralField(literal->value, 1, limits.field_values, field_counters);
        chargeLiteralString(*literal, strings);
        return;
    }
    invalid("node kind is outside the parser-produced CREATE TYPE surface");
}

void preflightAST(const ASTCreateTypeQuery & query, const DefinitionLoweringLimits & limits, StringBudget & strings)
{
    struct Pending
    {
        const IAST * node = nullptr;
        UInt64 depth = 0;
    };

    std::vector<Pending> pending;
    pending.reserve(static_cast<std::size_t>(std::min<UInt64>(limits.maximum_ast_nodes, 8'192)));
    pending.push_back({&query, 1});
    std::unordered_set<const IAST *> seen;
    seen.reserve(static_cast<std::size_t>(std::min<UInt64>(limits.maximum_ast_nodes, 8'192)));
    UInt64 edges = 0;
    FieldPreflight field_counters;

    while (!pending.empty())
    {
        const Pending current = pending.back();
        pending.pop_back();
        if (!current.node)
            invalid("AST contains a null node");
        if (current.depth > limits.maximum_ast_depth)
            limitExceeded("AST depth");
        if (seen.contains(current.node))
            invalid("AST contains a cycle or shared node");
        if (seen.size() >= limits.maximum_ast_nodes)
            limitExceeded("AST nodes");
        seen.emplace(current.node);
        validateASTNodeSurface(*current.node, limits, strings, field_counters);

        addProspectively(
            edges,
            checkedSize(current.node->children.size(), "AST child count does not fit UInt64"),
            limits.maximum_ast_edges,
            "AST edges");
        if (current.depth == std::numeric_limits<UInt64>::max() && !current.node->children.empty())
            limitExceeded("AST depth");
        for (auto child = current.node->children.rbegin(); child != current.node->children.rend(); ++child)
        {
            if (!*child)
                invalid("AST contains a null child");
            pending.push_back({child->get(), current.depth + 1});
        }
    }
}

ParameterKind lowerParameterKind(UDTParameterKind kind)
{
    switch (kind)
    {
        case UDTParameterKind::Type: return ParameterKind::Type;
        case UDTParameterKind::Bool: return ParameterKind::Bool;
        case UDTParameterKind::UInt8: return ParameterKind::UInt8;
        case UDTParameterKind::UInt16: return ParameterKind::UInt16;
        case UDTParameterKind::UInt32: return ParameterKind::UInt32;
        case UDTParameterKind::UInt64: return ParameterKind::UInt64;
        case UDTParameterKind::Int8: return ParameterKind::Int8;
        case UDTParameterKind::Int16: return ParameterKind::Int16;
        case UDTParameterKind::Int32: return ParameterKind::Int32;
        case UDTParameterKind::Int64: return ParameterKind::Int64;
        case UDTParameterKind::String: return ParameterKind::String;
    }
    invalid("unknown parser formal kind");
}

void validateParameterKind(ParameterKind kind)
{
    switch (kind)
    {
        case ParameterKind::Type:
        case ParameterKind::Bool:
        case ParameterKind::UInt8:
        case ParameterKind::UInt16:
        case ParameterKind::UInt32:
        case ParameterKind::UInt64:
        case ParameterKind::Int8:
        case ParameterKind::Int16:
        case ParameterKind::Int32:
        case ParameterKind::Int64:
        case ParameterKind::String: return;
    }
    invalid("unknown semantic formal kind");
}

const ASTExpressionList * getArguments(const ASTPtr & arguments)
{
    if (!arguments)
        return nullptr;
    const auto * list = arguments->as<ASTExpressionList>();
    if (!list || list->getSeparator() != ',')
        invalid("type arguments are not a canonical expression list");
    return list;
}

bool binaryNameLess(const AvailableDefinitionBinding & lhs, const AvailableDefinitionBinding & rhs)
{
    if (lhs.name.normalized_local_name != rhs.name.normalized_local_name)
        return lhs.name.normalized_local_name < rhs.name.normalized_local_name;
    if (lhs.identity.type_uuid != rhs.identity.type_uuid)
        return lhs.identity.type_uuid < rhs.identity.type_uuid;
    return lhs.identity.revision < rhs.identity.revision;
}

class Lowerer
{
public:
    Lowerer(
        const ASTCreateTypeQuery & query_,
        DefinitionIdentity identity_,
        const StructuredDefinitionName & definition_name_,
        const PreparedDefinitionLoweringBindings & prepared_bindings_,
        StringBudget & input_strings_)
        : query(query_)
        , identity(identity_)
        , definition_name(definition_name_)
        , prepared_bindings(prepared_bindings_)
        , limits(prepared_bindings_.getLimits())
        , input_strings(input_strings_)
        , output_strings(limits)
    {
    }

    DefinitionInput lower()
    {
        validateRequestAndBindings();
        validateQueryAndFormals();

        result.identity = identity;
        result.normalized_name = copyOutput(definition_name.normalized_qualified_name);
        result.normalized_local_name = copyOutput(definition_name.normalized_local_name);
        result.root = lowerExpression(query.definition, 1);
        if (result.root != 0)
            invalid("lowered definition root is not canonical node zero");
        result.policy_bearing = false;
        result.semantic_capabilities = 0;
        result.checker_abi = has_self_call ? 2 : 1;
        result.checker_charge_abi = 1;
        result.policy_abi = 1;
        result.function_registry_abi = 1;
        result.policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
        return std::move(result);
    }

private:
    const ASTCreateTypeQuery & query;
    const DefinitionIdentity identity;
    const StructuredDefinitionName & definition_name;
    const PreparedDefinitionLoweringBindings & prepared_bindings;
    const DefinitionLoweringLimits & limits;
    StringBudget & input_strings;
    StringBudget output_strings;
    DefinitionInput result;
    const AvailableDefinitionBinding * current_binding = nullptr;
    UInt64 output_edges = 0;
    FieldPreflight output_field_counters;
    bool has_self_call = false;

    String copyOutput(std::string_view value)
    {
        output_strings.charge(value);
        return String(value);
    }

    void validateRequestAndBindings()
    {
        if (identity.database_uuid == UUIDHelpers::Nil || identity.type_uuid == UUIDHelpers::Nil || identity.revision == 0)
            invalid("caller identity has a nil UUID or revision zero");
        validateStructuredName(definition_name, input_strings, "caller structured name is invalid or not canonical");
        if (prepared_bindings.getDatabaseUUID() != identity.database_uuid
            || prepared_bindings.getNormalizedDatabaseName() != definition_name.normalized_database_name)
            invalid("prepared bindings cross the caller database authority");

        const auto * identity_binding = prepared_bindings.findByIdentity(identity);
        const auto * name_binding = prepared_bindings.findByLocalName(definition_name.normalized_local_name);
        if ((identity_binding == nullptr) != (name_binding == nullptr) || (identity_binding && identity_binding != name_binding))
            invalid("current binding identity and structured name disagree");
        current_binding = identity_binding;
        if (!current_binding && prepared_bindings.getStatistics().validated_bindings >= limits.maximum_definitions)
            limitExceeded("definitions including the definition being lowered");
    }

    const AvailableDefinitionBinding * findBinding(std::string_view local_name) const
    {
        return prepared_bindings.findByLocalName(local_name);
    }

    const ASTIdentifier & requireShortIdentifier(const ASTPtr & ast, std::string_view what) const
    {
        const auto * identifier = ast ? ast->as<ASTIdentifier>() : nullptr;
        if (!identifier || identifier->isParam() || !identifier->isShort() || identifier->shortName().empty())
            invalid(what);
        return *identifier;
    }

    void validateQueryAndFormals()
    {
        const auto & local_identifier = requireShortIdentifier(query.type_name, "CREATE TYPE local name is not a short identifier");
        if (local_identifier.shortName() != definition_name.normalized_local_name)
            invalid("CREATE TYPE local name disagrees with the normalized caller boundary");
        if (query.database)
        {
            const auto & database = requireShortIdentifier(query.database, "CREATE TYPE database is not a short identifier");
            if (database.shortName() != definition_name.normalized_database_name)
                invalid("CREATE TYPE database disagrees with the normalized caller boundary");
        }
        if (!query.definition)
            invalid("CREATE TYPE has no definition");
        if (query.attach)
        {
            if (!query.uuid || !query.revision || !query.definition_hash || *query.uuid != identity.type_uuid
                || *query.revision != identity.revision || !isLowerHexDigest(*query.definition_hash))
                invalid("ATTACH TYPE internal identity fields disagree with the caller boundary");
        }
        else if (query.uuid || query.revision || query.definition_hash)
        {
            invalid("CREATE TYPE carries ATTACH-only internal fields");
        }
        if (query.comment)
        {
            const auto * comment = query.comment->as<ASTLiteral>();
            if (!comment || comment->value.getType() != Field::Types::String)
                invalid("CREATE TYPE comment is not a string literal");
        }

        const ASTExpressionList * declarations = getArguments(query.parameters);
        const std::size_t formal_count = declarations ? declarations->children.size() : 0;
        if (query.parameters && formal_count == 0)
            invalid("CREATE TYPE retains an empty formal declaration list");
        if (checkedSize(formal_count, "formal count does not fit UInt64") > limits.maximum_formals)
            limitExceeded("formals");
        result.parameters.reserve(formal_count);
        for (std::size_t index = 0; index < formal_count; ++index)
        {
            const auto * declaration = declarations->children[index]->as<ASTUDTParameterDeclaration>();
            if (!declaration || declaration->name.empty() || containsZero(declaration->name) || declaration->ordinal != index)
                invalid("formal declaration is malformed or has a non-dense ordinal");
            if (std::ranges::any_of(result.parameters, [&](const auto & prior) { return prior.normalized_name == declaration->name; }))
                invalid("formal declarations contain a duplicate name");
            result.parameters.push_back({
                .normalized_name = copyOutput(declaration->name),
                .kind = lowerParameterKind(declaration->kind),
            });
        }
        if (current_binding)
        {
            if (current_binding->parameter_kinds.size() != result.parameters.size())
                invalid("current binding formals disagree with the CREATE TYPE declaration");
            for (std::size_t index = 0; index < result.parameters.size(); ++index)
                if (current_binding->parameter_kinds[index] != result.parameters[index].kind)
                    invalid("current binding formals disagree with the CREATE TYPE declaration");
        }

        if (query.decreases)
        {
            const auto * reference = query.decreases->as<ASTUDTValueParameterReference>();
            if (!reference)
                invalid("DECREASES is not a value-formal reference");
            const UInt16 ordinal = validateValueReference(*reference);
            if (!isUnsignedIntegerParameter(result.parameters[ordinal].kind))
                invalid("DECREASES formal is not unsigned integral");
            result.decreasing_parameter = ordinal;
        }
    }

    TemplateNodeID addNode(TemplateNode node)
    {
        if (result.nodes.size() >= limits.maximum_output_nodes || result.nodes.size() >= std::numeric_limits<TemplateNodeID>::max())
            limitExceeded("output nodes");
        const auto id = static_cast<TemplateNodeID>(result.nodes.size());
        result.nodes.push_back(std::move(node));
        return id;
    }

    void addChild(TemplateNodeID parent, TemplateNodeID reference, std::string_view label = {})
    {
        if (parent >= result.nodes.size())
            invalid("lowerer produced an invalid parent node");
        addProspectively(output_edges, 1, limits.maximum_output_edges, "output edges");
        result.nodes[parent].children.push_back({.reference = reference, .label = copyOutput(label)});
    }

    UInt16 validateTypeReference(const ASTUDTTypeParameterReference & reference) const
    {
        if (reference.kind != UDTParameterKind::Type || reference.ordinal >= result.parameters.size()
            || result.parameters[reference.ordinal].kind != ParameterKind::Type
            || result.parameters[reference.ordinal].normalized_name != reference.name)
            invalid("TYPE formal reference disagrees with its declaration");
        return reference.ordinal;
    }

    UInt16 validateValueReference(const ASTUDTValueParameterReference & reference) const
    {
        if (reference.ordinal >= result.parameters.size() || result.parameters[reference.ordinal].kind == ParameterKind::Type
            || result.parameters[reference.ordinal].kind != lowerParameterKind(reference.kind)
            || result.parameters[reference.ordinal].normalized_name != reference.name)
            invalid("value formal reference disagrees with its declaration");
        return reference.ordinal;
    }

    TemplateNodeID lowerTypeReference(const ASTUDTTypeParameterReference & reference)
    {
        TemplateNode node;
        node.kind = TemplateNodeKind::TypeParameter;
        node.parameter = validateTypeReference(reference);
        return addNode(std::move(node));
    }

    TemplateNodeID lowerValueReference(const ASTUDTValueParameterReference & reference)
    {
        TemplateNode node;
        node.kind = TemplateNodeKind::ValueParameter;
        node.parameter = validateValueReference(reference);
        return addNode(std::move(node));
    }

    TemplateNodeID lowerLiteral(const ASTLiteral & literal)
    {
        TemplateNode node;
        switch (literal.value.getType())
        {
            case Field::Types::UInt64:
                node.kind = TemplateNodeKind::UnsignedLiteral;
                node.unsigned_literal = literal.value.safeGet<UInt64>();
                break;
            case Field::Types::Int64:
                node.kind = TemplateNodeKind::SignedLiteral;
                node.signed_literal = literal.value.safeGet<Int64>();
                break;
            case Field::Types::Bool:
                node.kind = TemplateNodeKind::BooleanLiteral;
                node.boolean_literal = literal.value.safeGet<bool>();
                break;
            case Field::Types::String:
                node.kind = TemplateNodeKind::StringLiteral;
                node.text = copyOutput(literal.value.safeGet<String>());
                break;
            default: invalid("literal kind is unsupported outside aggregate-function parameters");
        }
        return addNode(std::move(node));
    }

    TemplateNodeID lowerIdentifier(const ASTIdentifier & identifier)
    {
        if (identifier.isParam() || !identifier.isShort() || identifier.shortName().empty() || containsZero(identifier.shortName()))
            invalid("identifier argument is not a short parser identifier");
        TemplateNode node;
        node.kind = TemplateNodeKind::Identifier;
        node.text = copyOutput(identifier.shortName());
        return addNode(std::move(node));
    }

    TemplateNodeID lowerExpression(const ASTPtr & ast, UInt64 depth)
    {
        if (!ast)
            invalid("definition contains a null semantic node");
        if (depth > limits.maximum_ast_depth)
            limitExceeded("semantic lowering depth");
        if (const auto * enumeration = ast->as<ASTEnumDataType>())
            return lowerSpecializedEnum(*enumeration);
        if (const auto * tuple = ast->as<ASTTupleDataType>())
            return lowerSpecializedTuple(*tuple, depth);
        if (const auto * data_type = ast->as<ASTDataType>())
            return lowerDataType(*data_type, depth);
        if (const auto * reference = ast->as<ASTUDTReference>())
            return lowerDefinitionReference(*reference, depth);
        if (const auto * reference = ast->as<ASTUDTTypeParameterReference>())
            return lowerTypeReference(*reference);
        if (const auto * reference = ast->as<ASTUDTValueParameterReference>())
            return lowerValueReference(*reference);
        if (const auto * type_if = ast->as<ASTUDTTypeIf>())
            return lowerTypeIf(*type_if, depth);
        if (const auto * literal = ast->as<ASTLiteral>())
            return lowerLiteral(*literal);
        if (const auto * identifier = ast->as<ASTIdentifier>())
            return lowerIdentifier(*identifier);
        invalid("semantic node is unsupported in this data-type argument position");
    }

    TemplateNodeID lowerSpecializedTuple(const ASTTupleDataType & tuple, UInt64 depth)
    {
        const ASTExpressionList * arguments = getArguments(tuple.getArguments());
        const std::size_t count = arguments ? arguments->children.size() : 0;
        if (!tuple.element_names.empty())
        {
            if (tuple.element_names.size() != count
                || std::ranges::any_of(tuple.element_names, [](const String & name) { return name.empty() || containsZero(name); }))
                invalid("specialized Tuple has missing or invalid labels");
        }

        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = copyOutput("Tuple");
        const TemplateNodeID root = addNode(std::move(node));
        for (std::size_t index = 0; index < count; ++index)
        {
            const TemplateNodeID child = lowerExpression(arguments->children[index], depth + 1);
            addChild(root, child, tuple.element_names.empty() ? std::string_view{} : std::string_view(tuple.element_names[index]));
        }
        return root;
    }

    std::vector<std::pair<String, Int64>> readGenericEnumEntries(const ASTExpressionList & arguments)
    {
        if (arguments.children.empty())
            invalid("generic Enum has no entries");
        if (checkedSize(arguments.children.size(), "Enum entry count does not fit UInt64") > limits.maximum_enum_entries)
            limitExceeded("Enum entries");
        std::vector<std::pair<String, Int64>> entries;
        entries.reserve(arguments.children.size());
        Int64 implicit_base = 1;
        UInt64 implicit_offset = 0;
        bool first = true;
        for (const auto & entry : arguments.children)
        {
            if (const auto * literal = entry->as<ASTLiteral>())
            {
                if (literal->value.getType() != Field::Types::String)
                    invalid("implicit generic Enum entry is not a string literal");
                if (!first)
                    ++implicit_offset;
                if (implicit_offset > static_cast<UInt64>(std::numeric_limits<Int64>::max())
                    || implicit_base > std::numeric_limits<Int64>::max() - static_cast<Int64>(implicit_offset))
                    invalid("auto-assigned generic Enum value exceeds Int64");
                entries.emplace_back(literal->value.safeGet<String>(), implicit_base + static_cast<Int64>(implicit_offset));
            }
            else
            {
                const auto * function = entry->as<ASTFunction>();
                if (!function || function->name != "equals" || !function->isOperator() || !function->arguments
                    || function->arguments->children.size() != 2)
                    invalid("explicit generic Enum entry has an invalid shape");
                const auto * name = function->arguments->children[0]->as<ASTLiteral>();
                const auto * value = function->arguments->children[1]->as<ASTLiteral>();
                if (!name || !value || name->value.getType() != Field::Types::String
                    || (value->value.getType() != Field::Types::UInt64 && value->value.getType() != Field::Types::Int64))
                    invalid("explicit generic Enum entry has invalid literals");
                Int64 signed_value = 0;
                if (value->value.getType() == Field::Types::UInt64)
                {
                    const UInt64 unsigned_value = value->value.safeGet<UInt64>();
                    if (unsigned_value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                        invalid("explicit generic Enum value exceeds Int64");
                    signed_value = static_cast<Int64>(unsigned_value);
                }
                else
                    signed_value = value->value.safeGet<Int64>();
                if (first)
                    implicit_base = signed_value;
                entries.emplace_back(name->value.safeGet<String>(), signed_value);
            }
            first = false;
        }
        if (implicit_offset != 0 && implicit_offset != arguments.children.size() - 1)
            invalid("generic Enum mixes explicit values inside an implicit suffix");
        return entries;
    }

    TemplateNodeID lowerEnumValues(std::string_view family, std::vector<std::pair<String, Int64>> entries)
    {
        if (checkedSize(entries.size(), "Enum entry count does not fit UInt64") > limits.maximum_enum_entries)
            limitExceeded("Enum entries");
        bool enum16 = false;
        if (family == "Enum16")
            enum16 = true;
        else if (family == "Enum")
            enum16 = std::ranges::any_of(
                entries,
                [](const auto & entry)
                { return entry.second < std::numeric_limits<Int8>::min() || entry.second > std::numeric_limits<Int8>::max(); });
        else if (family != "Enum8")
            invalid("specialized Enum has an unknown canonical family");

        std::sort(entries.begin(), entries.end(), [](const auto & lhs, const auto & rhs) { return lhs.second < rhs.second; });
        TemplateNode node;
        node.kind = TemplateNodeKind::SpecializedEnum;
        node.specialized_enum_width = enum16 ? SpecializedEnumWidth::Enum16 : SpecializedEnumWidth::Enum8;
        node.enum_entries.reserve(entries.size());
        bool has_previous = false;
        Int64 previous = 0;
        for (auto & [name, value] : entries)
        {
            const bool in_range = enum16 ? value >= std::numeric_limits<Int16>::min() && value <= std::numeric_limits<Int16>::max()
                                         : value >= std::numeric_limits<Int8>::min() && value <= std::numeric_limits<Int8>::max();
            if (!in_range || (has_previous && value == previous))
                invalid("Enum values are duplicate or outside their canonical width");
            if (std::ranges::any_of(node.enum_entries, [&](const auto & prior) { return prior.name == name; }))
                invalid("Enum labels are not unique");
            node.enum_entries.push_back({.name = copyOutput(name), .value = value});
            previous = value;
            has_previous = true;
        }
        return addNode(std::move(node));
    }

    TemplateNodeID lowerSpecializedEnum(const ASTEnumDataType & enumeration)
    {
        const auto family = canonicalEnumFamily(enumeration.name);
        if (!family)
            invalid("specialized Enum family is unknown");
        if (checkedSize(enumeration.values.size(), "Enum entry count does not fit UInt64") > limits.maximum_enum_entries)
            limitExceeded("Enum entries");
        return lowerEnumValues(*family, enumeration.values);
    }

    TemplateNodeID lowerDataType(const ASTDataType & data_type, UInt64 depth)
    {
        const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type.name);
        if (!classification)
        {
            if (data_type.name == definition_name.normalized_local_name)
                invalid("self reference is not represented by its dedicated parser node");
            const auto * binding = findBinding(data_type.name);
            if (!binding)
                invalid("unqualified data-type family is neither built-in nor bound in this database");
            return lowerDefinitionCall(data_type.getArguments(), *binding);
        }

        const std::string_view canonical_family = classification.family->canonical_creator_name;
        const ASTExpressionList * arguments = getArguments(data_type.getArguments());
        if (classification.input_class == BuiltInDataTypeCreatorInputClass::CanonicalizeGenericEnumArguments)
        {
            if (!arguments)
                invalid("generic Enum has no arguments");
            return lowerEnumValues(canonical_family, readGenericEnumEntries(*arguments));
        }
        if (canonical_family == "Tuple" && arguments && !arguments->children.empty())
            invalid("nonempty Tuple did not use the parser's specialized node");
        if (canonical_family == "AggregateFunction" || canonical_family == "SimpleAggregateFunction")
            return lowerAggregateType(canonical_family, arguments, depth);
        if (canonical_family == "Dynamic")
            return lowerDynamic(arguments, depth);
        if (canonical_family == "JSON")
            return lowerJSON(arguments, depth);

        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = copyOutput(canonical_family);
        const TemplateNodeID root = addNode(std::move(node));
        if (!arguments)
            return root;
        for (const auto & argument : arguments->children)
        {
            if (const auto * pair = argument->as<ASTNameTypePair>())
            {
                if (canonical_family != "Nested" && canonical_family != "Tuple")
                    invalid("name/type pair is attached to a non-field built-in");
                const TemplateNodeID child = lowerExpression(pair->type, depth + 1);
                addChild(root, child, pair->name);
            }
            else
            {
                const TemplateNodeID child = lowerExpression(argument, depth + 1);
                addChild(root, child);
            }
        }
        return root;
    }

    TemplateNodeID lowerTypeIf(const ASTUDTTypeIf & type_if, UInt64 depth)
    {
        const auto * predicate = type_if.predicate->as<ASTUDTIsZero>();
        const auto * reference = predicate && predicate->parameter_reference
            ? predicate->parameter_reference->as<ASTUDTValueParameterReference>()
            : nullptr;
        if (!predicate || !reference)
            invalid("TYPE_IF predicate is not a value-formal zero test");
        const UInt16 parameter = validateValueReference(*reference);
        if (!isIntegerParameter(result.parameters[parameter].kind))
            invalid("TYPE_IF predicate formal is not integral");

        TemplateNode node;
        node.kind = TemplateNodeKind::TypeIfZero;
        node.parameter = parameter;
        const TemplateNodeID root = addNode(std::move(node));
        const TemplateNodeID zero_branch = lowerExpression(type_if.then_type, depth + 1);
        const TemplateNodeID positive_branch = lowerExpression(type_if.else_type, depth + 1);
        addChild(root, zero_branch);
        addChild(root, positive_branch);
        return root;
    }

    bool isSelfReference(const ASTUDTReference & reference) const noexcept
    {
        return reference.type_name == definition_name.normalized_local_name
            && (reference.database_name.empty() || reference.database_name == definition_name.normalized_database_name);
    }

    TemplateNodeID lowerDefinitionReference(const ASTUDTReference & reference, UInt64)
    {
        if (isSelfReference(reference))
            return lowerSelfCall(reference);
        if (reference.database_name.empty())
            invalid("external definition reference lacks its structured database component");
        if (reference.database_name != definition_name.normalized_database_name)
            invalid("definition reference crosses the database authority");
        const auto * binding = findBinding(reference.type_name);
        if (!binding)
            invalid("qualified definition reference is unbound in this database");
        return lowerDefinitionCall(reference.getArguments(), *binding);
    }

    UInt16 forwardedActual(const ASTPtr & argument, ParameterKind target_kind) const
    {
        if (target_kind == ParameterKind::Type)
        {
            const auto * reference = argument->as<ASTUDTTypeParameterReference>();
            if (!reference)
                invalid("definition-call TYPE actual is not a forwarded formal");
            return validateTypeReference(*reference);
        }
        const auto * reference = argument->as<ASTUDTValueParameterReference>();
        if (!reference)
            invalid("definition-call value actual is not a forwarded formal");
        const UInt16 ordinal = validateValueReference(*reference);
        if (result.parameters[ordinal].kind != target_kind)
            invalid("definition-call forwarded actual has the wrong kind");
        return ordinal;
    }

    UInt16 dependencyOrdinal(const AvailableDefinitionBinding & binding)
    {
        for (std::size_t index = 0; index < result.dependencies.size(); ++index)
        {
            const auto & dependency = result.dependencies[index];
            if (dependency.type_uuid == binding.identity.type_uuid && dependency.revision == binding.identity.revision)
                return static_cast<UInt16>(index);
        }
        if (result.dependencies.size() >= limits.maximum_dependencies || result.dependencies.size() >= std::numeric_limits<UInt16>::max())
            limitExceeded("direct dependencies");
        const auto ordinal = static_cast<UInt16>(result.dependencies.size());
        result.dependencies.push_back({
            .type_uuid = binding.identity.type_uuid,
            .revision = binding.identity.revision,
            .target_definition_hash = binding.definition_hash,
        });
        return ordinal;
    }

    TemplateNodeID lowerDefinitionCall(const ASTPtr & arguments_ast, const AvailableDefinitionBinding & binding)
    {
        if (binding.identity.type_uuid == identity.type_uuid && binding.identity.revision == identity.revision)
            invalid("self dependency must use the dedicated decreasing self-call shape");
        const ASTExpressionList * arguments = getArguments(arguments_ast);
        const std::size_t argument_count = arguments ? arguments->children.size() : 0;
        if (argument_count != binding.parameter_kinds.size())
            invalid("definition-call argument count does not match the target formals");

        TemplateNode node;
        node.kind = TemplateNodeKind::DefinitionCall;
        node.dependency_ordinal = dependencyOrdinal(binding);
        const TemplateNodeID root = addNode(std::move(node));
        for (std::size_t index = 0; index < argument_count; ++index)
            addChild(root, forwardedActual(arguments->children[index], binding.parameter_kinds[index]));
        return root;
    }

    void validateExactForwarding(const ASTPtr & argument, std::size_t ordinal, bool decreasing) const
    {
        const ParameterKind kind = result.parameters[ordinal].kind;
        if (kind == ParameterKind::Type)
        {
            const auto * reference = argument->as<ASTUDTTypeParameterReference>();
            if (decreasing || !reference || validateTypeReference(*reference) != ordinal)
                invalid("self-call TYPE actual does not forward the same formal");
            return;
        }
        if (decreasing)
        {
            const auto * decrement = argument->as<ASTUDTDecrement>();
            const auto * reference = decrement && decrement->parameter_reference
                ? decrement->parameter_reference->as<ASTUDTValueParameterReference>()
                : nullptr;
            if (!decrement || decrement->amount != 1 || !reference || validateValueReference(*reference) != ordinal)
                invalid("self-call decreasing actual is not the declared formal minus one");
            return;
        }
        const auto * reference = argument->as<ASTUDTValueParameterReference>();
        if (!reference || validateValueReference(*reference) != ordinal)
            invalid("self-call value actual does not forward the same formal");
    }

    TemplateNodeID lowerSelfCall(const ASTUDTReference & reference)
    {
        if (!result.decreasing_parameter)
            invalid("self call has no declared decreasing formal");
        const ASTExpressionList * arguments = getArguments(reference.getArguments());
        const std::size_t argument_count = arguments ? arguments->children.size() : 0;
        if (argument_count != result.parameters.size())
            invalid("self-call argument count does not match the current formals");
        for (std::size_t index = 0; index < argument_count; ++index)
            validateExactForwarding(arguments->children[index], index, index == *result.decreasing_parameter);

        TemplateNode node;
        node.kind = TemplateNodeKind::SelfCall;
        node.parameter = *result.decreasing_parameter;
        node.decrement = 1;
        has_self_call = true;
        return addNode(std::move(node));
    }

    TemplateNodeID appendFieldValue(const Field & field)
    {
        if (result.nodes.size() >= limits.maximum_output_nodes)
            limitExceeded("output nodes");
        if (output_edges >= limits.maximum_output_edges)
            limitExceeded("output edges");

        FieldPreflight next_field_counters = output_field_counters;
        preflightLiteralField(field, 1, limits.field_values, next_field_counters);

        CanonicalFieldValueLimits field_limits{
            .maximum_nodes = std::min<UInt64>(
                limits.field_values.maximum_nodes - output_field_counters.nodes, limits.maximum_output_nodes - result.nodes.size()),
            .maximum_edges
            = std::min<UInt64>(limits.field_values.maximum_edges - output_field_counters.edges, limits.maximum_output_edges - output_edges),
            .maximum_entries = limits.field_values.maximum_entries - output_field_counters.entries,
            .maximum_depth = limits.field_values.maximum_depth,
            .maximum_literal_bytes
            = std::min<UInt64>(limits.field_values.maximum_literal_bytes - output_field_counters.literal_bytes, output_strings.remaining()),
        };

        const std::size_t first = result.nodes.size();
        const auto roots = appendCanonicalFieldValues(std::span<const Field>(&field, 1), result.nodes, field_limits);
        if (roots.size() != 1)
            invalid("canonical Field lowering returned the wrong root count");
        UInt64 added_edges = 0;
        for (std::size_t index = first; index < result.nodes.size(); ++index)
        {
            const auto & node = result.nodes[index];
            addProspectively(
                added_edges,
                checkedSize(node.children.size(), "canonical Field edge count does not fit UInt64"),
                limits.maximum_output_edges - output_edges,
                "output Field edges");
            output_strings.charge(node.field_value.payload);
            output_strings.charge(node.field_value.name);
            for (const auto & child : node.children)
                output_strings.charge(child.label);
        }
        output_edges += added_edges;
        output_field_counters = next_field_counters;
        return roots.front();
    }

    std::optional<UInt16> findValueFormal(std::string_view name) const
    {
        for (std::size_t index = 0; index < result.parameters.size(); ++index)
            if (result.parameters[index].kind != ParameterKind::Type && result.parameters[index].normalized_name == name)
                return static_cast<UInt16>(index);
        return std::nullopt;
    }

    TemplateNodeID lowerAggregateFunction(const ASTPtr & function_ast)
    {
        TemplateNode node;
        node.kind = TemplateNodeKind::AggregateFunction;
        const ASTExpressionList * parameters = nullptr;
        if (const auto * identifier = function_ast->as<ASTIdentifier>())
        {
            if (identifier->isParam() || !identifier->isShort())
                invalid("aggregate-function name is not a short identifier");
            node.text = copyOutput(identifier->shortName());
        }
        else if (const auto * function = function_ast->as<ASTFunction>())
        {
            if (function->isOperator() || function->name.empty() || containsZero(function->name))
                invalid("aggregate-function name is empty or contains NUL");
            node.text = copyOutput(function->name);
            switch (function->getNullsAction())
            {
                case NullsAction::EMPTY: node.aggregate_nulls_action = AggregateFunctionNullsAction::Empty; break;
                case NullsAction::RESPECT_NULLS: node.aggregate_nulls_action = AggregateFunctionNullsAction::RespectNulls; break;
                case NullsAction::IGNORE_NULLS: node.aggregate_nulls_action = AggregateFunctionNullsAction::IgnoreNulls; break;
            }
            parameters = getArguments(function->arguments);
        }
        else
            invalid("aggregate-function name has an unsupported AST shape");

        const TemplateNodeID root = addNode(std::move(node));
        if (!parameters)
            return root;
        for (const auto & parameter : parameters->children)
        {
            TemplateNodeID child = 0;
            if (const auto * identifier = parameter->as<ASTIdentifier>())
            {
                const auto formal = identifier->isShort() ? findValueFormal(identifier->shortName()) : std::nullopt;
                if (!formal)
                    invalid("aggregate-function identifier parameter is not a declared value formal");
                TemplateNode formal_node;
                formal_node.kind = TemplateNodeKind::ValueParameter;
                formal_node.parameter = *formal;
                child = addNode(std::move(formal_node));
            }
            else
            {
                const Field value = parseFieldFromCastedLiteral(parameter);
                child = appendFieldValue(value);
            }
            addChild(root, child);
        }
        return root;
    }

    TemplateNodeID lowerAggregateType(std::string_view family, const ASTExpressionList * arguments, UInt64 depth)
    {
        if (!arguments || arguments->children.empty())
            invalid("aggregate-function type has no arguments");
        TemplateNode node;
        node.kind = TemplateNodeKind::BuiltIn;
        node.atom = copyOutput(family);
        const TemplateNodeID root = addNode(std::move(node));

        std::size_t function_index = 0;
        if (family == "AggregateFunction" && arguments->children.size() > 1)
        {
            const auto * version = arguments->children.front()->as<ASTLiteral>();
            if (version && version->value.getType() == Field::Types::UInt64)
            {
                addChild(root, lowerLiteral(*version));
                function_index = 1;
            }
        }
        if (function_index >= arguments->children.size())
            invalid("aggregate-function type has no function name");
        addChild(root, lowerAggregateFunction(arguments->children[function_index]));
        for (std::size_t index = function_index + 1; index < arguments->children.size(); ++index)
            addChild(root, lowerExpression(arguments->children[index], depth + 1));
        return root;
    }

    std::pair<String, ASTPtr> readEquals(const ASTPtr & ast, std::string_view what) const
    {
        const auto * function = ast ? ast->as<ASTFunction>() : nullptr;
        if (!function || function->name != "equals" || !function->isOperator() || !function->arguments
            || function->arguments->children.size() != 2)
            invalid(what);
        const auto * identifier = function->arguments->children[0]->as<ASTIdentifier>();
        if (!identifier || identifier->isParam() || !identifier->isShort())
            invalid(what);
        return {identifier->shortName(), function->arguments->children[1]};
    }

    TemplateNodeID lowerUnsignedSettingValue(const ASTPtr & value)
    {
        if (const auto * literal = value->as<ASTLiteral>())
        {
            if (literal->value.getType() != Field::Types::UInt64)
                invalid("named setting value is not unsigned");
            return lowerLiteral(*literal);
        }
        const auto * reference = value->as<ASTUDTValueParameterReference>();
        if (!reference)
            invalid("named setting value is not an unsigned literal or formal");
        const UInt16 ordinal = validateValueReference(*reference);
        if (!isUnsignedIntegerParameter(result.parameters[ordinal].kind))
            invalid("named setting formal is not unsigned");
        return lowerValueReference(*reference);
    }

    TemplateNodeID lowerDynamic(const ASTExpressionList * arguments, UInt64)
    {
        TemplateNode root_node;
        root_node.kind = TemplateNodeKind::BuiltIn;
        root_node.atom = copyOutput("Dynamic");
        const TemplateNodeID root = addNode(std::move(root_node));
        if (!arguments)
            return root;
        if (arguments->children.size() != 1)
            invalid("Dynamic has more than one setting");
        auto [name, value] = readEquals(arguments->children.front(), "Dynamic setting has an invalid equals shape");
        if (name != "max_types")
            invalid("Dynamic setting name is not canonical");
        TemplateNode setting;
        setting.kind = TemplateNodeKind::DynamicSetting;
        setting.text = copyOutput(name);
        const TemplateNodeID setting_id = addNode(std::move(setting));
        addChild(setting_id, lowerUnsignedSettingValue(value));
        addChild(root, setting_id);
        return root;
    }

    struct ObjectArgumentView
    {
        const ASTObjectTypeArgument * ast = nullptr;
        UInt8 rank = 0;
        String key;
    };

    ObjectArgumentView inspectObjectArgument(const ASTPtr & ast) const
    {
        const auto * argument = ast ? ast->as<ASTObjectTypeArgument>() : nullptr;
        if (!argument)
            invalid("JSON child is not an object argument");
        if (argument->parameter)
        {
            auto [name, value] = readEquals(argument->parameter, "JSON setting has an invalid equals shape");
            static_cast<void>(value);
            if (name != "max_dynamic_types" && name != "max_dynamic_paths")
                invalid("JSON setting name is not canonical");
            return {.ast = argument, .rank = static_cast<UInt8>(name == "max_dynamic_types" ? 0 : 1), .key = std::move(name)};
        }
        if (argument->path_with_type)
        {
            const auto * path = argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!path || path->path.empty() || containsZero(path->path))
                invalid("JSON typed path is empty or malformed");
            return {.ast = argument, .rank = 2, .key = path->path};
        }
        if (argument->skip_path)
        {
            const auto * path = argument->skip_path->as<ASTIdentifier>();
            if (!path || path->isParam() || path->name().empty() || containsZero(path->name()))
                invalid("JSON skipped path is empty or malformed");
            return {.ast = argument, .rank = 3, .key = path->name()};
        }
        const auto * regexp = argument->skip_path_regexp ? argument->skip_path_regexp->as<ASTLiteral>() : nullptr;
        if (!regexp || regexp->value.getType() != Field::Types::String)
            invalid("JSON skipped regexp is not a string literal");
        return {.ast = argument, .rank = 4, .key = regexp->value.safeGet<String>()};
    }

    TemplateNodeID lowerObjectArgument(const ObjectArgumentView & view, UInt64 depth)
    {
        TemplateNode node;
        if (view.ast->parameter)
        {
            auto [name, value] = readEquals(view.ast->parameter, "JSON setting has an invalid equals shape");
            node.kind = TemplateNodeKind::ObjectSetting;
            node.text = copyOutput(name);
            const TemplateNodeID root = addNode(std::move(node));
            addChild(root, lowerUnsignedSettingValue(value));
            return root;
        }
        if (view.ast->path_with_type)
        {
            const auto & path = view.ast->path_with_type->as<ASTObjectTypedPathArgument &>();
            node.kind = TemplateNodeKind::ObjectTypedPath;
            node.text = copyOutput(path.path);
            const TemplateNodeID root = addNode(std::move(node));
            addChild(root, lowerExpression(path.type, depth + 1));
            return root;
        }
        if (view.ast->skip_path)
        {
            node.kind = TemplateNodeKind::ObjectSkipPath;
            node.text = copyOutput(view.key);
            return addNode(std::move(node));
        }
        node.kind = TemplateNodeKind::ObjectSkipRegexp;
        node.text = copyOutput(view.key);
        return addNode(std::move(node));
    }

    TemplateNodeID lowerJSON(const ASTExpressionList * arguments, UInt64 depth)
    {
        TemplateNode root_node;
        root_node.kind = TemplateNodeKind::BuiltIn;
        root_node.atom = copyOutput("JSON");
        const TemplateNodeID root = addNode(std::move(root_node));
        if (!arguments)
            return root;

        std::vector<ObjectArgumentView> ordered;
        ordered.reserve(arguments->children.size());
        for (const auto & argument : arguments->children)
            ordered.push_back(inspectObjectArgument(argument));
        std::stable_sort(
            ordered.begin(),
            ordered.end(),
            [](const auto & lhs, const auto & rhs)
            {
                if (lhs.rank != rhs.rank)
                    return lhs.rank < rhs.rank;
                return lhs.key < rhs.key;
            });
        for (std::size_t index = 1; index < ordered.size(); ++index)
            if (ordered[index - 1].rank == ordered[index].rank && ordered[index - 1].key == ordered[index].key && ordered[index].rank != 4)
                invalid("JSON arguments contain a duplicate canonical key");
        for (const auto & argument : ordered)
            addChild(root, lowerObjectArgument(argument, depth));
        return root;
    }
};

}

PreparedDefinitionLoweringBindings::PreparedDefinitionLoweringBindings(
    UUID database_uuid_,
    String normalized_database_name_,
    DefinitionLoweringLimits limits_,
    std::vector<AvailableDefinitionBinding> bindings_,
    std::vector<std::size_t> name_order_,
    std::vector<std::size_t> identity_order_,
    DefinitionLoweringBindingPreparationStatistics statistics_)
    : database_uuid(database_uuid_)
    , normalized_database_name(std::move(normalized_database_name_))
    , limits(std::move(limits_))
    , bindings(std::move(bindings_))
    , name_order(std::move(name_order_))
    , identity_order(std::move(identity_order_))
    , statistics(statistics_)
{
}

const AvailableDefinitionBinding *
PreparedDefinitionLoweringBindings::findByLocalName(std::string_view normalized_local_name) const noexcept
{
    const auto found = std::lower_bound(
        name_order.begin(),
        name_order.end(),
        normalized_local_name,
        [&](std::size_t index, std::string_view value) { return bindings[index].name.normalized_local_name < value; });
    if (found == name_order.end() || bindings[*found].name.normalized_local_name != normalized_local_name)
        return nullptr;
    return &bindings[*found];
}

const AvailableDefinitionBinding * PreparedDefinitionLoweringBindings::findByIdentity(const DefinitionIdentity & identity) const noexcept
{
    if (identity.database_uuid != database_uuid)
        return nullptr;
    const auto found = std::lower_bound(
        identity_order.begin(),
        identity_order.end(),
        identity,
        [&](std::size_t index, const DefinitionIdentity & value)
        {
            const auto & candidate = bindings[index].identity;
            if (candidate.type_uuid != value.type_uuid)
                return candidate.type_uuid < value.type_uuid;
            return candidate.revision < value.revision;
        });
    if (found == identity_order.end() || bindings[*found].identity != identity)
        return nullptr;
    return &bindings[*found];
}

PreparedDefinitionLoweringBindings prepareDefinitionLoweringBindings(
    UUID database_uuid,
    String normalized_database_name,
    std::vector<AvailableDefinitionBinding> bindings,
    const DefinitionLoweringLimits & limits)
{
    validateLimits(limits);
    if (database_uuid == UUIDHelpers::Nil || normalized_database_name.empty() || containsZero(normalized_database_name))
        invalid("prepared binding catalog has an invalid database boundary");
    if (checkedSize(bindings.size(), "binding count does not fit UInt64") > limits.maximum_definitions)
        limitExceeded("available bindings");

    StringBudget catalog_strings(limits.maximum_string_bytes, limits.maximum_catalog_string_bytes);
    catalog_strings.charge(normalized_database_name);
    std::vector<std::size_t> name_order(bindings.size());
    std::iota(name_order.begin(), name_order.end(), 0);
    for (const auto & binding : bindings)
    {
        validateStructuredName(binding.name, catalog_strings, "available binding name is invalid or not canonical");
        if (binding.name.normalized_database_name != normalized_database_name || binding.identity.database_uuid != database_uuid
            || binding.identity.type_uuid == UUIDHelpers::Nil || binding.identity.revision == 0)
            invalid("available binding crosses the database authority or has an invalid identity");
        if (checkedSize(binding.parameter_kinds.size(), "binding formal count does not fit UInt64") > limits.maximum_formals)
            limitExceeded("binding formals");
        for (const auto kind : binding.parameter_kinds)
            validateParameterKind(kind);
    }

    std::sort(
        name_order.begin(),
        name_order.end(),
        [&](std::size_t lhs, std::size_t rhs) { return binaryNameLess(bindings[lhs], bindings[rhs]); });
    for (std::size_t index = 1; index < name_order.size(); ++index)
    {
        const auto & previous = bindings[name_order[index - 1]];
        const auto & current = bindings[name_order[index]];
        if (previous.name.normalized_local_name == current.name.normalized_local_name)
            invalid("available bindings contain a duplicate local name");
    }

    std::vector<std::size_t> identity_order(name_order);
    std::sort(
        identity_order.begin(),
        identity_order.end(),
        [&](std::size_t lhs, std::size_t rhs)
        {
            const auto & left = bindings[lhs].identity;
            const auto & right = bindings[rhs].identity;
            if (left.type_uuid != right.type_uuid)
                return left.type_uuid < right.type_uuid;
            return left.revision < right.revision;
        });
    for (std::size_t index = 1; index < identity_order.size(); ++index)
    {
        const auto & previous = bindings[identity_order[index - 1]].identity;
        const auto & current = bindings[identity_order[index]].identity;
        if (previous.type_uuid == current.type_uuid && previous.revision == current.revision)
            invalid("available bindings contain a duplicate immutable identity");
    }

    const UInt64 binding_count = checkedSize(bindings.size(), "binding count does not fit UInt64");
    return PreparedDefinitionLoweringBindings(
        database_uuid,
        std::move(normalized_database_name),
        limits,
        std::move(bindings),
        std::move(name_order),
        std::move(identity_order),
        {
            .validated_bindings = binding_count,
            .catalog_string_bytes = catalog_strings.chargedBytes(),
            .name_index_entries = binding_count,
            .identity_index_entries = binding_count,
        });
}

DefinitionInput lowerCreateTypeQueryToDefinitionInput(
    const ASTCreateTypeQuery & query,
    DefinitionIdentity identity,
    const StructuredDefinitionName & name,
    const PreparedDefinitionLoweringBindings & prepared_bindings)
{
    const auto & limits = prepared_bindings.getLimits();
    validateLimits(limits);
    StringBudget input_strings(limits);
    preflightAST(query, limits, input_strings);
    return Lowerer(query, identity, name, prepared_bindings, input_strings).lower();
}

DefinitionInput lowerCreateTypeQueryToDefinitionInput(
    const ASTCreateTypeQuery & query, const DefinitionLoweringRequest & request, const DefinitionLoweringLimits & limits)
{
    std::vector<AvailableDefinitionBinding> owned_bindings;
    owned_bindings.reserve(request.available_bindings.size());
    for (const auto & binding : request.available_bindings)
        owned_bindings.push_back(binding);
    auto prepared_bindings = prepareDefinitionLoweringBindings(
        request.identity.database_uuid, request.name.normalized_database_name, std::move(owned_bindings), limits);
    return lowerCreateTypeQueryToDefinitionInput(query, request.identity, request.name, prepared_bindings);
}

DefinitionInput definitionInputFromCheckedDefinition(const Definition & definition)
{
    DefinitionInput result;
    result.identity = definition.getIdentity();
    result.normalized_name = definition.getNormalizedName();
    result.normalized_local_name = definition.getNormalizedLocalName();
    result.parameters = definition.getParameters();
    result.decreasing_parameter = definition.getDecreasingParameter();
    result.nodes = definition.getNodes();
    result.root = definition.getRoot();
    result.policy_bearing = definition.isPolicyBearing();
    result.semantic_capabilities = definition.getSemanticCapabilities();
    result.checker_abi = definition.getCheckerABI();
    result.checker_charge_abi = definition.getCheckerChargeABI();
    result.policy_abi = definition.getPolicyABI();
    result.function_registry_abi = definition.getFunctionRegistryABI();
    result.policy_semantic_hash = definition.getPolicySemanticHash();
    result.dependencies = definition.getDependencies();
    return result;
}

}
