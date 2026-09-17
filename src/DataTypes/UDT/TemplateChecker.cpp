#include <DataTypes/UDT/TemplateChecker.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/UDT/CanonicalHash.h>

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <memory_resource>
#include <numeric>
#include <optional>
#include <span>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
extern const int TOO_MANY_BYTES;
}

namespace DB::UDT
{
namespace
{

constexpr UInt16 CHECKER_ABI_ACYCLIC = 1;
constexpr UInt16 CHECKER_ABI_DECREASING_SELF = 2;
constexpr UInt16 CHECKER_CHARGE_ABI = 1;
constexpr UInt16 POLICY_ABI = 1;
constexpr UInt16 FUNCTION_REGISTRY_ABI = 1;

bool isRegisteredBuiltInFamily(std::string_view family_name) noexcept
{
    return static_cast<bool>(BuiltInDataTypeFamilyClassifier::classifyGeneric(family_name));
}

bool collidesWithRegisteredBuiltInFamilyOrAlias(std::string_view family_name) noexcept
{
    return BuiltInDataTypeFamilyClassifier::collidesWithRegisteredFamilyOrAlias(family_name);
}

[[noreturn]] void invalid(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid user-defined type definition: {}", message);
}

[[noreturn]] void limitExceeded(std::string_view message)
{
    throw Exception(ErrorCodes::TOO_MANY_BYTES, "User-defined type checker limit exceeded: {}", message);
}

[[noreturn]] void invariantViolation(std::string_view message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "User-defined type checker invariant failed: {}", message);
}

UInt64 checkedSize(std::size_t size, std::string_view description)
{
    if (!std::in_range<UInt64>(size))
        limitExceeded(description);
    return static_cast<UInt64>(size);
}

void addProspectively(UInt64 & current, UInt64 amount, UInt64 maximum, std::string_view description)
{
    if (amount > maximum || current > maximum - amount)
        limitExceeded(description);
    current += amount;
}

bool isZeroDigest(const Digest & digest)
{
    return std::all_of(digest.begin(), digest.end(), [](CanonicalByte byte) { return byte == 0; });
}

bool containsZero(std::string_view value)
{
    return value.find('\0') != std::string_view::npos;
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
    invalid("unknown parameter kind");
}

void validateLimits(const TemplateCheckerLimits & limits)
{
    if (limits.maximum_formals > std::numeric_limits<UInt16>::max())
        invalid("maximum_formals exceeds the canonical UInt16 domain");
    if (limits.maximum_direct_dependencies > std::numeric_limits<UInt16>::max())
        invalid("maximum_direct_dependencies exceeds the canonical UInt16 domain");
    if (limits.maximum_template_nodes >= std::numeric_limits<TemplateNodeID>::max())
        invalid("maximum_template_nodes exceeds the canonical node-ID domain");
    if (!std::in_range<std::size_t>(limits.maximum_scratch_bytes))
        invalid("maximum_scratch_bytes does not fit the host allocation domain");
}

class QuotaMemoryResource final : public std::pmr::memory_resource
{
public:
    explicit QuotaMemoryResource(UInt64 maximum_bytes_)
        : maximum_bytes(maximum_bytes_)
    {
    }

    UInt64 getPeakBytes() const noexcept { return peak_bytes; }

private:
    void * do_allocate(std::size_t bytes, std::size_t alignment) override
    {
        const UInt64 requested = checkedSize(bytes, "scratch allocation does not fit UInt64");
        if (requested > maximum_bytes || current_bytes > maximum_bytes - requested)
            limitExceeded("scratch bytes");
        void * result = std::pmr::new_delete_resource()->allocate(bytes, alignment);
        current_bytes += requested;
        peak_bytes = std::max(peak_bytes, current_bytes);
        return result;
    }

    void do_deallocate(void * pointer, std::size_t bytes, std::size_t alignment) override
    {
        const UInt64 released = static_cast<UInt64>(bytes);
        if (released > current_bytes)
            std::terminate();
        current_bytes -= released;
        std::pmr::new_delete_resource()->deallocate(pointer, bytes, alignment);
    }

    bool do_is_equal(const std::pmr::memory_resource & rhs) const noexcept override { return this == &rhs; }

    UInt64 maximum_bytes;
    UInt64 current_bytes = 0;
    UInt64 peak_bytes = 0;
};

class CatalogWorkBudget final
{
public:
    explicit CatalogWorkBudget(UInt64 maximum_)
        : maximum(maximum_)
    {
    }

    void charge(UInt64 amount = 1) { addProspectively(current, amount, maximum, "catalog checker work"); }

    UInt64 getCurrent() const noexcept { return current; }

private:
    UInt64 maximum;
    UInt64 current = 0;
};

class DefinitionWorkBudget final
{
public:
    DefinitionWorkBudget(UInt64 maximum_, CatalogWorkBudget & catalog_)
        : maximum(maximum_)
        , catalog(catalog_)
    {
    }

    void charge(UInt64 amount = 1)
    {
        if (amount > maximum || current > maximum - amount)
            limitExceeded("definition checker work");
        catalog.charge(amount);
        current += amount;
    }

    UInt64 getCurrent() const noexcept { return current; }

private:
    UInt64 maximum;
    UInt64 current = 0;
    CatalogWorkBudget & catalog;
};

struct TargetKey
{
    UUID type_uuid = UUIDHelpers::Nil;
    UInt64 revision = 0;

    bool operator==(const TargetKey &) const = default;
};

UInt64 mix(UInt64 value) noexcept
{
    value ^= value >> 30;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27;
    value *= 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

struct TargetKeyHash
{
    std::size_t operator()(const TargetKey & key) const noexcept
    {
        UInt64 result = mix(UUIDHelpers::getHighBytes(key.type_uuid));
        result = mix(result ^ UUIDHelpers::getLowBytes(key.type_uuid));
        return static_cast<std::size_t>(mix(result ^ key.revision));
    }
};

struct DefinitionIdentityHash
{
    std::size_t operator()(const DefinitionIdentity & identity) const noexcept
    {
        UInt64 result = mix(UUIDHelpers::getHighBytes(identity.database_uuid));
        result = mix(result ^ UUIDHelpers::getLowBytes(identity.database_uuid));
        result = mix(result ^ UUIDHelpers::getHighBytes(identity.type_uuid));
        result = mix(result ^ UUIDHelpers::getLowBytes(identity.type_uuid));
        return static_cast<std::size_t>(mix(result ^ identity.revision));
    }
};

bool dependencyLess(const DefinitionDependency & lhs, const DefinitionDependency & rhs)
{
    const auto lhs_uuid = uuidToCanonicalBytes(lhs.type_uuid);
    const auto rhs_uuid = uuidToCanonicalBytes(rhs.type_uuid);
    if (lhs_uuid != rhs_uuid)
        return std::lexicographical_compare(lhs_uuid.begin(), lhs_uuid.end(), rhs_uuid.begin(), rhs_uuid.end());
    return lhs.revision < rhs.revision;
}

UInt64 countDefinitionInputBytes(const DefinitionInput & input, UInt64 maximum)
{
    UInt64 total = 0;
    const auto add = [&](UInt64 amount) { addProspectively(total, amount, maximum, "definition accepted-input bytes"); };
    const auto addString = [&](std::string_view value)
    {
        add(sizeof(UInt64));
        add(checkedSize(value.size(), "accepted-input string length"));
    };

    add(2 * sizeof(UUID) + sizeof(UInt64));
    addString(input.normalized_name);
    addString(
        input.normalized_local_name.empty() ? std::string_view(input.normalized_name) : std::string_view(input.normalized_local_name));
    add(sizeof(UInt64));
    for (const auto & parameter : input.parameters)
    {
        add(sizeof(UInt8));
        addString(parameter.normalized_name);
    }
    add(sizeof(UInt8) + sizeof(UInt16));
    add(sizeof(UInt64) + sizeof(TemplateNodeID));
    for (const auto & node : input.nodes)
    {
        add(5 * sizeof(UInt8) + 2 * sizeof(UInt16) + 2 * sizeof(UInt64) + sizeof(Int64));
        addString(node.atom);
        addString(node.text);
        addString(node.field_value.payload);
        addString(node.field_value.name);
        add(sizeof(UInt64));
        for (const auto & entry : node.enum_entries)
        {
            addString(entry.name);
            add(sizeof(Int64));
        }
        add(sizeof(UInt64));
        for (const auto & child : node.children)
        {
            add(sizeof(TemplateNodeID));
            addString(child.label);
        }
    }
    add(2 * sizeof(UInt8) + 4 * sizeof(UInt16) + sizeof(Digest));
    add(sizeof(UInt64));
    for (std::size_t index = 0; index < input.dependencies.size(); ++index)
        add(sizeof(UUID) + sizeof(UInt64) + sizeof(Digest));
    return total;
}

struct PreflightResult
{
    UInt64 accepted_input_bytes = 0;
    UInt64 maximum_definition_input_bytes = 0;
    UInt64 catalog_nodes = 0;
    UInt64 catalog_dependency_edges = 0;
};

PreflightResult preflightInputs(const std::vector<DefinitionInput> & inputs, const TemplateCheckerLimits & limits)
{
    if (checkedSize(inputs.size(), "definition count") > limits.maximum_definitions)
        limitExceeded("definition count");

    PreflightResult result;
    for (const auto & input : inputs)
    {
        const UInt64 input_bytes = countDefinitionInputBytes(input, limits.maximum_definition_input_bytes);
        result.maximum_definition_input_bytes = std::max(result.maximum_definition_input_bytes, input_bytes);
        addProspectively(result.accepted_input_bytes, input_bytes, limits.maximum_catalog_input_bytes, "catalog accepted-input bytes");

        const UInt64 parameter_count = checkedSize(input.parameters.size(), "formal count");
        if (parameter_count > limits.maximum_formals)
            limitExceeded("formal count");
        for (const auto & parameter : input.parameters)
        {
            if (checkedSize(parameter.normalized_name.size(), "formal name bytes") > limits.maximum_formal_name_bytes)
                limitExceeded("formal name bytes");
        }
        const UInt64 node_count = checkedSize(input.nodes.size(), "template node count");
        if (node_count > limits.maximum_template_nodes)
            limitExceeded("template node count");
        addProspectively(result.catalog_nodes, node_count, limits.maximum_catalog_nodes, "catalog template nodes");
        const UInt64 dependency_count = checkedSize(input.dependencies.size(), "direct dependency count");
        if (dependency_count > limits.maximum_direct_dependencies)
            limitExceeded("direct dependency count");

        UInt64 definition_edges = 0;
        UInt64 ir_atom_bytes = 0;
        UInt64 ir_literal_bytes = 0;
        UInt64 ir_enum_entries = 0;
        for (const auto & node : input.nodes)
        {
            addProspectively(
                definition_edges,
                checkedSize(node.children.size(), "template child count"),
                limits.maximum_template_edges,
                "template edges");
            if (node.kind == TemplateNodeKind::BuiltIn)
            {
                addProspectively(
                    ir_atom_bytes,
                    checkedSize(node.atom.size(), "canonical IR atom bytes"),
                    limits.maximum_ir_atom_bytes,
                    "canonical IR atom bytes");
                for (const auto & child : node.children)
                    addProspectively(
                        ir_atom_bytes,
                        checkedSize(child.label.size(), "canonical IR field-label bytes"),
                        limits.maximum_ir_atom_bytes,
                        "canonical IR atom bytes");
            }
            if (node.kind == TemplateNodeKind::StringLiteral || node.kind == TemplateNodeKind::Identifier
                || node.kind == TemplateNodeKind::AggregateFunction || node.kind == TemplateNodeKind::DynamicSetting
                || node.kind == TemplateNodeKind::ObjectSetting || node.kind == TemplateNodeKind::ObjectTypedPath
                || node.kind == TemplateNodeKind::ObjectSkipPath || node.kind == TemplateNodeKind::ObjectSkipRegexp)
            {
                const UInt64 text_bytes = checkedSize(node.text.size(), "canonical IR literal bytes");
                if ((node.kind == TemplateNodeKind::Identifier || node.kind == TemplateNodeKind::AggregateFunction
                     || node.kind == TemplateNodeKind::DynamicSetting || node.kind == TemplateNodeKind::ObjectSetting)
                    && text_bytes > limits.maximum_ir_identifier_bytes)
                    limitExceeded("canonical IR identifier bytes");
                addProspectively(ir_literal_bytes, text_bytes, limits.maximum_ir_literal_bytes, "canonical IR literal bytes");
            }
            if (node.kind == TemplateNodeKind::FieldValue)
            {
                addProspectively(
                    ir_literal_bytes,
                    checkedSize(node.field_value.payload.size(), "canonical Field payload bytes"),
                    limits.maximum_ir_literal_bytes,
                    "canonical IR literal bytes");
                addProspectively(
                    ir_literal_bytes,
                    checkedSize(node.field_value.name.size(), "canonical Field name bytes"),
                    limits.maximum_ir_literal_bytes,
                    "canonical IR literal bytes");
                if (node.field_value.kind == CanonicalFieldKind::Object)
                {
                    for (const auto & child : node.children)
                        addProspectively(
                            ir_literal_bytes,
                            checkedSize(child.label.size(), "canonical Object Field key bytes"),
                            limits.maximum_ir_literal_bytes,
                            "canonical IR literal bytes");
                }
            }
            if (node.kind == TemplateNodeKind::SpecializedEnum)
            {
                addProspectively(
                    ir_enum_entries,
                    checkedSize(node.enum_entries.size(), "canonical IR Enum entry count"),
                    limits.maximum_ir_enum_entries,
                    "canonical IR Enum entry count");
                for (const auto & entry : node.enum_entries)
                    addProspectively(
                        ir_literal_bytes,
                        checkedSize(entry.name.size(), "canonical IR Enum label bytes"),
                        limits.maximum_ir_literal_bytes,
                        "canonical IR literal bytes");
            }
        }
        addProspectively(result.catalog_dependency_edges, dependency_count, limits.maximum_catalog_edges, "catalog dependency edges");
    }
    return result;
}

bool hasTemplateEdges(TemplateNodeKind kind)
{
    return kind == TemplateNodeKind::BuiltIn || kind == TemplateNodeKind::TypeIfZero || kind == TemplateNodeKind::AggregateFunction
        || kind == TemplateNodeKind::DynamicSetting || kind == TemplateNodeKind::ObjectSetting || kind == TemplateNodeKind::ObjectTypedPath
        || kind == TemplateNodeKind::FieldValue;
}

bool producesType(TemplateNodeKind kind)
{
    switch (kind)
    {
        case TemplateNodeKind::BuiltIn:
        case TemplateNodeKind::TypeParameter:
        case TemplateNodeKind::SpecializedEnum:
        case TemplateNodeKind::TypeIfZero:
        case TemplateNodeKind::SelfCall:
        case TemplateNodeKind::DefinitionCall: return true;
        case TemplateNodeKind::ValueParameter:
        case TemplateNodeKind::UnsignedLiteral:
        case TemplateNodeKind::BooleanLiteral:
        case TemplateNodeKind::SignedLiteral:
        case TemplateNodeKind::StringLiteral:
        case TemplateNodeKind::Identifier:
        case TemplateNodeKind::FieldValue:
        case TemplateNodeKind::AggregateFunction:
        case TemplateNodeKind::DynamicSetting:
        case TemplateNodeKind::ObjectSetting:
        case TemplateNodeKind::ObjectTypedPath:
        case TemplateNodeKind::ObjectSkipPath:
        case TemplateNodeKind::ObjectSkipRegexp: return false;
    }
    return false;
}

CheckerProof::UUID toProofUUID(const UUID & uuid)
{
    return uuidToCanonicalBytes(uuid);
}

CheckerProof::EncodingLimits makeEncodingLimits(const TemplateCheckerLimits & limits)
{
    return {
        .maximum_output_bytes = limits.maximum_canonical_definition_bytes,
        .maximum_formals = limits.maximum_formals,
        .maximum_formal_name_bytes = limits.maximum_formal_name_bytes,
        .maximum_template_ir_bytes = limits.maximum_canonical_definition_bytes,
        .maximum_direct_dependencies = limits.maximum_direct_dependencies,
        .maximum_transitive_dependencies = limits.maximum_transitive_dependencies,
        .maximum_logical_nodes = limits.maximum_logical_node_occurrences,
        .maximum_template_depth = limits.maximum_template_depth,
        .maximum_template_edges = limits.maximum_template_edges,
        .maximum_ir_atom_bytes = limits.maximum_ir_atom_bytes,
        .maximum_ir_literal_bytes = limits.maximum_ir_literal_bytes,
        .maximum_ir_identifier_bytes = limits.maximum_ir_identifier_bytes,
        .maximum_ir_enum_entries = limits.maximum_ir_enum_entries,
        .maximum_checker_charge_units = limits.maximum_checker_work,
    };
}

template <typename Encoder>
std::pmr::vector<CheckerProof::Byte> encodeTemporary(Encoder && encoder, std::pmr::memory_resource * scratch)
{
    const std::size_t encoded_size = encoder(std::span<CheckerProof::Byte>{});
    std::pmr::vector<CheckerProof::Byte> result(scratch);
    result.resize(encoded_size);
    const std::size_t written = encoder(std::span<CheckerProof::Byte>(result.data(), result.size()));
    if (written != result.size())
        invariantViolation("canonical encoder returned an inconsistent size");
    return result;
}

Digest digestEncoded(const std::pmr::vector<CheckerProof::Byte> & encoded)
{
    return sha256(std::span<const CanonicalByte>(encoded.data(), encoded.size()));
}

struct DefinitionShape
{
    UInt64 logical_occurrences = 0;
    UInt64 maximum_node_depth = 0;
    UInt64 self_call_occurrences = 0;
};

DefinitionShape validateTemplateDAG(
    const DefinitionInput & definition,
    DefinitionWorkBudget & work,
    const TemplateCheckerLimits & limits,
    std::pmr::memory_resource * scratch)
{
    const std::size_t node_count = definition.nodes.size();
    std::pmr::vector<UInt64> indegree(node_count, 0, scratch);
    std::pmr::vector<UInt64> depth(node_count, 0, scratch);
    std::pmr::vector<UInt64> occurrences(node_count, 0, scratch);

    work.charge(checkedSize(node_count, "template node count"));
    for (const auto & node : definition.nodes)
    {
        if (!hasTemplateEdges(node.kind))
            continue;
        for (const auto & child : node.children)
        {
            work.charge();
            if (child.reference >= node_count)
                invalid("template child node is out of range");
            if (indegree[child.reference] == std::numeric_limits<UInt64>::max())
                limitExceeded("template indegree overflow");
            ++indegree[child.reference];
        }
    }

    std::pmr::vector<TemplateNodeID> ready(scratch);
    ready.reserve(node_count);
    for (std::size_t index = 0; index < node_count; ++index)
    {
        work.charge();
        if (indegree[index] == 0)
            ready.push_back(static_cast<TemplateNodeID>(index));
    }
    occurrences[definition.root] = 1;
    depth[definition.root] = 1;

    std::size_t cursor = 0;
    std::size_t visited = 0;
    DefinitionShape shape;
    while (cursor < ready.size())
    {
        const TemplateNodeID current = ready[cursor++];
        ++visited;
        work.charge();
        if (occurrences[current] != 0)
        {
            if (depth[current] > limits.maximum_template_depth)
                limitExceeded("template depth");
            shape.maximum_node_depth = std::max(shape.maximum_node_depth, depth[current]);
            addProspectively(
                shape.logical_occurrences, occurrences[current], limits.maximum_logical_node_occurrences, "logical node occurrences");
            if (definition.nodes[current].kind == TemplateNodeKind::SelfCall)
                addProspectively(
                    shape.self_call_occurrences, occurrences[current], limits.maximum_logical_node_occurrences, "self-call occurrences");
        }

        const auto & node = definition.nodes[current];
        if (!hasTemplateEdges(node.kind))
            continue;
        for (const auto & child : node.children)
        {
            work.charge();
            if (occurrences[current] != 0)
            {
                if (depth[current] == std::numeric_limits<UInt64>::max())
                    limitExceeded("template depth overflow");
                depth[child.reference] = std::max(depth[child.reference], depth[current] + 1);
                addProspectively(
                    occurrences[child.reference],
                    occurrences[current],
                    limits.maximum_logical_node_occurrences,
                    "logical node occurrences");
            }
            if (--indegree[child.reference] == 0)
                ready.push_back(child.reference);
        }
    }

    if (visited != node_count)
        invalid("template-node cycle");
    return shape;
}

bool hasInactiveLeafState(const TemplateNode & node)
{
    return !node.atom.empty() || node.decrement != 0 || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal
        || !node.text.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
        || node.field_value.kind != CanonicalFieldKind::None || !node.field_value.payload.empty() || !node.field_value.name.empty()
        || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty || node.dependency_ordinal != 0 || !node.children.empty();
}

bool hasInactiveTypedSurfaceState(const TemplateNode & node)
{
    return node.field_value.kind != CanonicalFieldKind::None || !node.field_value.payload.empty() || !node.field_value.name.empty()
        || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty;
}

CheckerProof::CanonicalFieldKind toCanonicalFieldKind(CanonicalFieldKind kind)
{
    switch (kind)
    {
        case CanonicalFieldKind::None: return CheckerProof::CanonicalFieldKind::None;
        case CanonicalFieldKind::Null: return CheckerProof::CanonicalFieldKind::Null;
        case CanonicalFieldKind::UInt64: return CheckerProof::CanonicalFieldKind::UInt64;
        case CanonicalFieldKind::Int64: return CheckerProof::CanonicalFieldKind::Int64;
        case CanonicalFieldKind::Float64: return CheckerProof::CanonicalFieldKind::Float64;
        case CanonicalFieldKind::String: return CheckerProof::CanonicalFieldKind::String;
        case CanonicalFieldKind::Bool: return CheckerProof::CanonicalFieldKind::Bool;
        case CanonicalFieldKind::UInt128: return CheckerProof::CanonicalFieldKind::UInt128;
        case CanonicalFieldKind::Int128: return CheckerProof::CanonicalFieldKind::Int128;
        case CanonicalFieldKind::UInt256: return CheckerProof::CanonicalFieldKind::UInt256;
        case CanonicalFieldKind::Int256: return CheckerProof::CanonicalFieldKind::Int256;
        case CanonicalFieldKind::Decimal32: return CheckerProof::CanonicalFieldKind::Decimal32;
        case CanonicalFieldKind::Decimal64: return CheckerProof::CanonicalFieldKind::Decimal64;
        case CanonicalFieldKind::Decimal128: return CheckerProof::CanonicalFieldKind::Decimal128;
        case CanonicalFieldKind::Decimal256: return CheckerProof::CanonicalFieldKind::Decimal256;
        case CanonicalFieldKind::UUID: return CheckerProof::CanonicalFieldKind::UUID;
        case CanonicalFieldKind::IPv4: return CheckerProof::CanonicalFieldKind::IPv4;
        case CanonicalFieldKind::IPv6: return CheckerProof::CanonicalFieldKind::IPv6;
        case CanonicalFieldKind::NegativeInfinity: return CheckerProof::CanonicalFieldKind::NegativeInfinity;
        case CanonicalFieldKind::PositiveInfinity: return CheckerProof::CanonicalFieldKind::PositiveInfinity;
        case CanonicalFieldKind::Array: return CheckerProof::CanonicalFieldKind::Array;
        case CanonicalFieldKind::Tuple: return CheckerProof::CanonicalFieldKind::Tuple;
        case CanonicalFieldKind::Map: return CheckerProof::CanonicalFieldKind::Map;
        case CanonicalFieldKind::Object: return CheckerProof::CanonicalFieldKind::Object;
        case CanonicalFieldKind::AggregateFunctionState: return CheckerProof::CanonicalFieldKind::AggregateFunctionState;
    }
    invariantViolation("unknown canonical Field kind");
}

CheckerProof::AggregateFunctionNullsAction toCanonicalNullsAction(AggregateFunctionNullsAction action)
{
    switch (action)
    {
        case AggregateFunctionNullsAction::Empty: return CheckerProof::AggregateFunctionNullsAction::Empty;
        case AggregateFunctionNullsAction::RespectNulls: return CheckerProof::AggregateFunctionNullsAction::RespectNulls;
        case AggregateFunctionNullsAction::IgnoreNulls: return CheckerProof::AggregateFunctionNullsAction::IgnoreNulls;
    }
    invariantViolation("unknown aggregate-function NULL action");
}

std::span<const CheckerProof::Byte> fieldPayload(const CanonicalFieldValue & field)
{
    return {
        reinterpret_cast<const CheckerProof::Byte *>(field.payload.data()),
        field.payload.size(),
    };
}

bool equalsASCIICaseInsensitive(std::string_view lhs, std::string_view rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (std::size_t index = 0; index < lhs.size(); ++index)
    {
        const auto fold = [](unsigned char character)
        { return character >= 'A' && character <= 'Z' ? static_cast<unsigned char>(character + ('a' - 'A')) : character; };
        if (fold(static_cast<unsigned char>(lhs[index])) != fold(static_cast<unsigned char>(rhs[index])))
            return false;
    }
    return true;
}

bool isCompositeField(CanonicalFieldKind kind)
{
    return kind == CanonicalFieldKind::Array || kind == CanonicalFieldKind::Tuple || kind == CanonicalFieldKind::Map
        || kind == CanonicalFieldKind::Object;
}

bool binaryStringLess(std::string_view lhs, std::string_view rhs)
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool binaryStringLessOrEqual(std::string_view lhs, std::string_view rhs)
{
    return lhs == rhs || binaryStringLess(lhs, rhs);
}

void validateBuiltInChildLabels(const TemplateNode & node, DefinitionWorkBudget & work, std::pmr::memory_resource * scratch)
{
    const bool tuple = node.atom == "Tuple";
    const bool nested = node.atom == "Nested";
    if (!tuple && !nested)
    {
        for (const auto & child : node.children)
        {
            work.charge();
            if (!child.label.empty())
                invalid("only Tuple/Nested type arguments may carry field labels");
        }
        return;
    }

    bool has_label = false;
    bool has_unlabelled = false;
    for (const auto & child : node.children)
    {
        work.charge();
        if (containsZero(child.label))
            invalid("Tuple/Nested field label contains NUL");
        has_label |= !child.label.empty();
        has_unlabelled |= child.label.empty();
    }
    if (nested && node.children.empty())
        invalid("Nested type has no named fields");
    if ((nested || has_label) && has_unlabelled)
        invalid("Tuple/Nested field labels are only partially specified");

    if (!has_label)
        return;
    std::pmr::unordered_set<std::string_view> labels(scratch);
    labels.reserve(node.children.size());
    for (const auto & child : node.children)
    {
        work.charge();
        if (!labels.emplace(child.label).second)
            invalid("Tuple/Nested field labels are not unique");
    }
}

UInt8 objectArgumentRank(const TemplateNode & node)
{
    switch (node.kind)
    {
        case TemplateNodeKind::ObjectSetting: return node.text == "max_dynamic_types" ? 0 : 1;
        case TemplateNodeKind::ObjectTypedPath: return 2;
        case TemplateNodeKind::ObjectSkipPath: return 3;
        case TemplateNodeKind::ObjectSkipRegexp: return 4;
        default: invalid("JSON/Object argument kind is outside its parser surface");
    }
}

void validateCanonicalObjectArgumentOrder(
    const DefinitionInput & definition, const TemplateNode & node, DefinitionWorkBudget & work)
{
    UInt8 previous_rank = 0;
    std::string_view previous_text;
    bool has_previous = false;
    for (const auto & child : node.children)
    {
        work.charge();
        const auto & argument = definition.nodes[child.reference];
        const UInt8 rank = objectArgumentRank(argument);
        if (has_previous && rank < previous_rank)
            invalid("JSON/Object arguments are not in canonical factory order");
        if (has_previous && rank == previous_rank)
        {
            const bool ordered = argument.kind == TemplateNodeKind::ObjectSkipRegexp ? binaryStringLessOrEqual(previous_text, argument.text)
                                                                                     : binaryStringLess(previous_text, argument.text);
            if (!ordered)
                invalid("JSON/Object arguments are not in canonical factory order");
        }
        previous_rank = rank;
        previous_text = argument.text;
        has_previous = true;
    }
}

struct ProofVisit
{
    TemplateNodeID node = 0;
    bool positive_guard = false;
    bool base_branch = false;
};

struct LocalValidationResult
{
    bool has_self_call = false;
};

LocalValidationResult validateDefinitionSemantics(
    const DefinitionInput & definition,
    std::span<const std::size_t> dependency_targets,
    const std::vector<DefinitionInput> & definitions,
    TemplateChecker::BuiltInFamilyPredicateForTest is_registered_family,
    DefinitionWorkBudget & work,
    std::pmr::memory_resource * scratch)
{
    std::pmr::unordered_set<std::string_view> parameter_names(scratch);
    parameter_names.reserve(definition.parameters.size());
    for (const auto & parameter : definition.parameters)
    {
        work.charge();
        validateParameterKind(parameter.kind);
        if (parameter.normalized_name.empty() || containsZero(parameter.normalized_name))
            invalid("parameter name is empty or contains NUL");
        if (!parameter_names.emplace(parameter.normalized_name).second)
            invalid("duplicate parameter name");
    }

    if (definition.decreasing_parameter)
    {
        if (*definition.decreasing_parameter >= definition.parameters.size())
            invalid("decreasing parameter is out of range");
        if (!isUnsignedIntegerParameter(definition.parameters[*definition.decreasing_parameter].kind))
            invalid("decreasing parameter is not unsigned integral");
    }

    std::pmr::vector<ProofVisit> pending(scratch);
    pending.push_back({definition.root, false, false});
    std::pmr::unordered_set<UInt64> visited_states(scratch);
    std::pmr::vector<UInt8> reachable(definition.nodes.size(), 0, scratch);
    bool has_self_call = false;
    bool has_reachable_base = false;

    while (!pending.empty())
    {
        const ProofVisit visit = pending.back();
        pending.pop_back();
        work.charge();
        if (visit.node >= definition.nodes.size())
            invalid("template node is out of range");
        reachable[visit.node] = 1;
        const UInt64 state_key = (static_cast<UInt64>(visit.node) << 2) | static_cast<UInt64>(visit.positive_guard)
            | (static_cast<UInt64>(visit.base_branch) << 1);
        if (!visited_states.emplace(state_key).second)
            continue;

        const auto & node = definition.nodes[visit.node];
        if (static_cast<UInt8>(node.kind) > static_cast<UInt8>(TemplateNodeKind::ObjectSkipRegexp))
            invalid("unknown template node kind");
        switch (node.kind)
        {
            case TemplateNodeKind::BuiltIn:
                if (node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || !node.text.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || containsZero(node.atom)
                    || !is_registered_family(node.atom))
                    invalid("built-in node is noncanonical or names an unregistered family");

                validateBuiltInChildLabels(node, work, scratch);

                if (node.atom == "AggregateFunction" || node.atom == "SimpleAggregateFunction")
                {
                    if (node.children.empty())
                        invalid("aggregate-function type has no function node");
                    std::size_t function_index = 0;
                    if (node.atom == "AggregateFunction" && node.children.size() > 1)
                    {
                        const auto & possible_version = definition.nodes[node.children.front().reference];
                        if (possible_version.kind == TemplateNodeKind::UnsignedLiteral
                            || (possible_version.kind == TemplateNodeKind::ValueParameter
                                && possible_version.parameter < definition.parameters.size()
                                && isUnsignedIntegerParameter(definition.parameters[possible_version.parameter].kind)))
                            function_index = 1;
                    }
                    if (function_index >= node.children.size()
                        || definition.nodes[node.children[function_index].reference].kind != TemplateNodeKind::AggregateFunction)
                        invalid("aggregate-function type has an invalid function-name/parameter node");
                    const auto & function_node = definition.nodes[node.children[function_index].reference];
                    if (node.atom == "SimpleAggregateFunction"
                        && function_node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                        invalid("SimpleAggregateFunction cannot carry an ignored NULL action");
                    if (node.atom == "SimpleAggregateFunction" && node.children.size() == 1)
                        invalid("SimpleAggregateFunction has no storage argument type");
                    for (std::size_t index = 0; index < node.children.size(); ++index)
                    {
                        if (!node.children[index].label.empty())
                            invalid("aggregate-function type argument has a field label");
                        if (index > function_index && !producesType(definition.nodes[node.children[index].reference].kind))
                            invalid("aggregate-function argument does not produce a type");
                    }
                }
                else if (node.atom == "Dynamic")
                {
                    if (node.children.size() > 1
                        || (!node.children.empty()
                            && definition.nodes[node.children.front().reference].kind != TemplateNodeKind::DynamicSetting))
                        invalid("Dynamic has an invalid typed setting node");
                }
                else if (equalsASCIICaseInsensitive(node.atom, "JSON"))
                {
                    for (const auto & child : node.children)
                    {
                        const auto kind = definition.nodes[child.reference].kind;
                        if (!child.label.empty()
                            || (kind != TemplateNodeKind::ObjectSetting && kind != TemplateNodeKind::ObjectTypedPath
                                && kind != TemplateNodeKind::ObjectSkipPath && kind != TemplateNodeKind::ObjectSkipRegexp))
                            invalid("JSON/Object has an invalid typed argument node");
                    }
                    validateCanonicalObjectArgumentOrder(definition, node, work);
                }
                else
                {
                    for (const auto & child : node.children)
                    {
                        const auto kind = definition.nodes[child.reference].kind;
                        if (kind == TemplateNodeKind::FieldValue || kind == TemplateNodeKind::AggregateFunction
                            || kind == TemplateNodeKind::DynamicSetting || kind == TemplateNodeKind::ObjectSetting
                            || kind == TemplateNodeKind::ObjectTypedPath || kind == TemplateNodeKind::ObjectSkipPath
                            || kind == TemplateNodeKind::ObjectSkipRegexp)
                            invalid("typed parser-surface node is attached to the wrong built-in family");
                    }
                }
                for (auto child = node.children.rbegin(); child != node.children.rend(); ++child)
                {
                    work.charge();
                    pending.push_back({child->reference, visit.positive_guard, visit.base_branch});
                }
                break;
            case TemplateNodeKind::TypeParameter: {
                const UInt16 parameter = node.parameter;
                if (hasInactiveLeafState(node) || parameter >= definition.parameters.size()
                    || definition.parameters[parameter].kind != ParameterKind::Type)
                    invalid("invalid type-parameter use");
                break;
            }
            case TemplateNodeKind::ValueParameter: {
                const UInt16 parameter = node.parameter;
                if (hasInactiveLeafState(node) || parameter >= definition.parameters.size()
                    || definition.parameters[parameter].kind == ParameterKind::Type)
                    invalid("invalid value-parameter use");
                break;
            }
            case TemplateNodeKind::UnsignedLiteral: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.signed_literal != 0 || node.boolean_literal
                    || !node.text.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("unsigned literal carries inactive fields");
                break;
            }
            case TemplateNodeKind::BooleanLiteral: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || !node.text.empty() || node.specialized_enum_width != SpecializedEnumWidth::None
                    || !node.enum_entries.empty() || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0
                    || !node.children.empty())
                    invalid("Boolean literal carries inactive fields");
                break;
            }
            case TemplateNodeKind::SignedLiteral: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0 || node.boolean_literal
                    || !node.text.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("signed literal carries inactive fields");
                break;
            }
            case TemplateNodeKind::StringLiteral: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.specialized_enum_width != SpecializedEnumWidth::None
                    || !node.enum_entries.empty() || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0
                    || !node.children.empty())
                    invalid("string literal carries inactive fields");
                break;
            }
            case TemplateNodeKind::Identifier: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.specialized_enum_width != SpecializedEnumWidth::None
                    || !node.enum_entries.empty() || node.dependency_ordinal != 0 || !node.children.empty() || node.text.empty()
                    || containsZero(node.text) || hasInactiveTypedSurfaceState(node))
                    invalid("identifier argument is empty or contains NUL");
                break;
            }
            case TemplateNodeKind::SpecializedEnum: {
                const auto width = node.specialized_enum_width;
                const auto & entries = node.enum_entries;
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || !node.text.empty() || node.dependency_ordinal != 0
                    || !node.children.empty() || (width != SpecializedEnumWidth::Enum8 && width != SpecializedEnumWidth::Enum16)
                    || entries.empty() || hasInactiveTypedSurfaceState(node))
                    invalid("specialized Enum width or entries are invalid");
                std::pmr::unordered_set<std::string_view> names(scratch);
                names.reserve(entries.size());
                bool has_previous = false;
                Int64 previous = 0;
                for (const auto & entry : entries)
                {
                    work.charge();
                    const bool in_range = width == SpecializedEnumWidth::Enum8
                        ? entry.value >= std::numeric_limits<Int8>::min() && entry.value <= std::numeric_limits<Int8>::max()
                        : entry.value >= std::numeric_limits<Int16>::min() && entry.value <= std::numeric_limits<Int16>::max();
                    if (!in_range || (has_previous && entry.value <= previous))
                        invalid("specialized Enum values are out of range or not strictly increasing");
                    if (!names.emplace(entry.name).second)
                        invalid("specialized Enum labels are not unique");
                    has_previous = true;
                    previous = entry.value;
                }
                break;
            }
            case TemplateNodeKind::FieldValue: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || !node.text.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty || node.dependency_ordinal != 0)
                    invalid("Field-value node carries inactive fields");
                CheckerProof::validateCanonicalFieldValue(
                    toCanonicalFieldKind(node.field_value.kind), fieldPayload(node.field_value), node.field_value.name);
                if (isCompositeField(node.field_value.kind))
                {
                    if (node.field_value.kind == CanonicalFieldKind::Map && (node.children.size() % 2) != 0)
                        invalid("Map Field has odd key/value arity");
                    std::string_view previous_object_key;
                    bool has_previous_object_key = false;
                    for (auto child = node.children.rbegin(); child != node.children.rend(); ++child)
                    {
                        work.charge();
                        if (definition.nodes[child->reference].kind != TemplateNodeKind::FieldValue)
                            invalid("composite Field child is not a Field value");
                        if (node.field_value.kind == CanonicalFieldKind::Object)
                        {
                            if (has_previous_object_key && !binaryStringLess(child->label, previous_object_key))
                                invalid("Object Field keys are not in strict binary order");
                            previous_object_key = child->label;
                            has_previous_object_key = true;
                        }
                        else if (!child->label.empty())
                        {
                            invalid("non-Object Field edge has a label");
                        }
                        pending.push_back({child->reference, visit.positive_guard, visit.base_branch});
                    }
                }
                else if (!node.children.empty())
                {
                    invalid("scalar/opaque Field has structural children");
                }
                break;
            }
            case TemplateNodeKind::AggregateFunction: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.text.empty() || containsZero(node.text)
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || node.field_value.kind != CanonicalFieldKind::None || !node.field_value.payload.empty()
                    || !node.field_value.name.empty() || node.dependency_ordinal != 0)
                    invalid("aggregate-function node is noncanonical");
                static_cast<void>(toCanonicalNullsAction(node.aggregate_nulls_action));
                for (auto child = node.children.rbegin(); child != node.children.rend(); ++child)
                {
                    work.charge();
                    const auto & parameter_node = definition.nodes[child->reference];
                    if (!child->label.empty()
                        || (parameter_node.kind != TemplateNodeKind::FieldValue && parameter_node.kind != TemplateNodeKind::ValueParameter))
                        invalid("aggregate-function parameter is not a Field value or value parameter");
                    pending.push_back({child->reference, visit.positive_guard, visit.base_branch});
                }
                break;
            }
            case TemplateNodeKind::DynamicSetting:
            case TemplateNodeKind::ObjectSetting: {
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.text.empty() || containsZero(node.text)
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || node.children.size() != 1
                    || !node.children.front().label.empty())
                    invalid("named-setting node is noncanonical");
                if ((node.kind == TemplateNodeKind::DynamicSetting && node.text != "max_types")
                    || (node.kind == TemplateNodeKind::ObjectSetting && node.text != "max_dynamic_types"
                        && node.text != "max_dynamic_paths"))
                    invalid("named-setting node has an unknown name");
                const auto & value_node = definition.nodes[node.children.front().reference];
                if (value_node.kind != TemplateNodeKind::UnsignedLiteral
                    && (value_node.kind != TemplateNodeKind::ValueParameter || value_node.parameter >= definition.parameters.size()
                        || !isUnsignedIntegerParameter(definition.parameters[value_node.parameter].kind)))
                    invalid("named setting requires an unsigned value");
                pending.push_back({node.children.front().reference, visit.positive_guard, visit.base_branch});
                break;
            }
            case TemplateNodeKind::ObjectTypedPath:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.text.empty() || containsZero(node.text)
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || node.children.size() != 1
                    || !node.children.front().label.empty() || !producesType(definition.nodes[node.children.front().reference].kind))
                    invalid("typed Object path is noncanonical or has no type");
                pending.push_back({node.children.front().reference, visit.positive_guard, visit.base_branch});
                break;
            case TemplateNodeKind::ObjectSkipPath:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.text.empty() || containsZero(node.text)
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("skipped Object path is noncanonical");
                break;
            case TemplateNodeKind::ObjectSkipRegexp:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.specialized_enum_width != SpecializedEnumWidth::None
                    || !node.enum_entries.empty() || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0
                    || !node.children.empty())
                    invalid("skipped Object regexp is noncanonical");
                break;
            case TemplateNodeKind::TypeIfZero: {
                if (!node.atom.empty() || node.parameter >= definition.parameters.size() || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || !node.text.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || node.children.size() != 2
                    || !node.children[0].label.empty() || !node.children[1].label.empty())
                    invalid("invalid TYPE_IF_ZERO node");
                const ParameterKind parameter_kind = definition.parameters[node.parameter].kind;
                if (!isIntegerParameter(parameter_kind))
                    invalid("TYPE_IF_ZERO requires an integer parameter");
                if (!producesType(definition.nodes[node.children[0].reference].kind)
                    || !producesType(definition.nodes[node.children[1].reference].kind))
                    invalid("TYPE_IF_ZERO branch does not produce a type");
                const bool guards_measure = definition.decreasing_parameter && node.parameter == *definition.decreasing_parameter;
                work.charge(2);
                pending.push_back({node.children[1].reference, visit.positive_guard || guards_measure, visit.base_branch});
                pending.push_back({node.children[0].reference, visit.positive_guard, visit.base_branch || guards_measure});
                break;
            }
            case TemplateNodeKind::SelfCall: {
                has_self_call = true;
                const UInt16 parameter = node.parameter;
                const UInt64 decrement = node.decrement;
                if (!node.atom.empty() || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal
                    || !node.text.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || node.dependency_ordinal != 0 || !node.children.empty()
                    || !definition.decreasing_parameter || parameter != *definition.decreasing_parameter || decrement != 1
                    || !visit.positive_guard || visit.base_branch)
                    invalid("self call is not strictly guarded by zero and decreasing by one");
                break;
            }
            case TemplateNodeKind::DefinitionCall: {
                const UInt16 dependency_ordinal = node.dependency_ordinal;
                const auto children = std::span(node.children);
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || !node.text.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || hasInactiveTypedSurfaceState(node) || dependency_ordinal >= dependency_targets.size())
                    invalid("definition-call dependency ordinal is out of range");
                const auto & target_parameters = definitions[dependency_targets[dependency_ordinal]].parameters;
                if (children.size() != target_parameters.size())
                    invalid("definition-call argument count mismatch");
                for (std::size_t index = 0; index < children.size(); ++index)
                {
                    work.charge();
                    if (!children[index].label.empty() || children[index].reference >= definition.parameters.size()
                        || definition.parameters[children[index].reference].kind != target_parameters[index].kind)
                        invalid("definition-call argument kind mismatch");
                }
                break;
            }
        }

        if (visit.base_branch && node.kind != TemplateNodeKind::SelfCall)
            has_reachable_base = true;
    }

    if (std::find(reachable.begin(), reachable.end(), UInt8{0}) != reachable.end())
        invalid("unreachable template node");
    if (has_self_call && !has_reachable_base)
        invalid("recursive template has no reachable base case");
    if (has_self_call && definition.policy_bearing)
        invalid("policy-bearing self recursion is forbidden");
    if (!has_self_call && definition.decreasing_parameter)
        invalid("DECREASES is declared but unused");
    return {.has_self_call = has_self_call};
}

CheckerProof::CanonicalIRNodeKind toCanonicalKind(TemplateNodeKind kind)
{
    switch (kind)
    {
        case TemplateNodeKind::BuiltIn: return CheckerProof::CanonicalIRNodeKind::BuiltIn;
        case TemplateNodeKind::TypeParameter: return CheckerProof::CanonicalIRNodeKind::TypeFormal;
        case TemplateNodeKind::ValueParameter: return CheckerProof::CanonicalIRNodeKind::ValueFormal;
        case TemplateNodeKind::UnsignedLiteral: return CheckerProof::CanonicalIRNodeKind::UnsignedLiteral;
        case TemplateNodeKind::BooleanLiteral: return CheckerProof::CanonicalIRNodeKind::BooleanLiteral;
        case TemplateNodeKind::SignedLiteral: return CheckerProof::CanonicalIRNodeKind::SignedLiteral;
        case TemplateNodeKind::StringLiteral: return CheckerProof::CanonicalIRNodeKind::StringLiteral;
        case TemplateNodeKind::Identifier: return CheckerProof::CanonicalIRNodeKind::Identifier;
        case TemplateNodeKind::SpecializedEnum: return CheckerProof::CanonicalIRNodeKind::SpecializedEnum;
        case TemplateNodeKind::FieldValue: return CheckerProof::CanonicalIRNodeKind::FieldValue;
        case TemplateNodeKind::AggregateFunction: return CheckerProof::CanonicalIRNodeKind::AggregateFunction;
        case TemplateNodeKind::DynamicSetting: return CheckerProof::CanonicalIRNodeKind::DynamicSetting;
        case TemplateNodeKind::ObjectSetting: return CheckerProof::CanonicalIRNodeKind::ObjectSetting;
        case TemplateNodeKind::ObjectTypedPath: return CheckerProof::CanonicalIRNodeKind::ObjectTypedPath;
        case TemplateNodeKind::ObjectSkipPath: return CheckerProof::CanonicalIRNodeKind::ObjectSkipPath;
        case TemplateNodeKind::ObjectSkipRegexp: return CheckerProof::CanonicalIRNodeKind::ObjectSkipRegexp;
        case TemplateNodeKind::TypeIfZero: return CheckerProof::CanonicalIRNodeKind::TypeIfZero;
        case TemplateNodeKind::SelfCall: return CheckerProof::CanonicalIRNodeKind::SelfCall;
        case TemplateNodeKind::DefinitionCall: return CheckerProof::CanonicalIRNodeKind::ExternalCall;
    }
    invariantViolation("unknown template node kind after validation");
}

CheckerProof::SpecializedEnumWidth toCanonicalEnumWidth(SpecializedEnumWidth width)
{
    switch (width)
    {
        case SpecializedEnumWidth::None: return CheckerProof::SpecializedEnumWidth::None;
        case SpecializedEnumWidth::Enum8: return CheckerProof::SpecializedEnumWidth::Enum8;
        case SpecializedEnumWidth::Enum16: return CheckerProof::SpecializedEnumWidth::Enum16;
    }
    invariantViolation("unknown specialized Enum width after validation");
}

String encodeCanonicalIR(
    const DefinitionInput & definition,
    DefinitionWorkBudget & work,
    const CheckerProof::EncodingLimits & encoding_limits,
    std::pmr::memory_resource * scratch)
{
    constexpr TemplateNodeID unassigned = std::numeric_limits<TemplateNodeID>::max();
    std::pmr::vector<TemplateNodeID> canonical_ids(definition.nodes.size(), unassigned, scratch);
    std::pmr::vector<TemplateNodeID> order(scratch);
    order.reserve(definition.nodes.size());
    canonical_ids[definition.root] = 0;
    order.push_back(definition.root);

    work.charge(checkedSize(definition.nodes.size(), "template node count"));
    for (std::size_t cursor = 0; cursor < order.size(); ++cursor)
    {
        work.charge();
        const auto & node = definition.nodes[order[cursor]];
        if (!hasTemplateEdges(node.kind))
            continue;
        for (const auto & child : node.children)
        {
            work.charge();
            if (canonical_ids[child.reference] == unassigned)
            {
                if (order.size() >= unassigned)
                    limitExceeded("canonical node-ID domain");
                canonical_ids[child.reference] = static_cast<TemplateNodeID>(order.size());
                order.push_back(child.reference);
            }
        }
    }
    if (order.size() != definition.nodes.size())
        invariantViolation("canonical traversal disagrees with reachability validation");

    std::size_t child_count = 0;
    std::size_t enum_entry_count = 0;
    for (const TemplateNodeID source : order)
    {
        const auto & node = definition.nodes[source];
        if (hasTemplateEdges(node.kind) || node.kind == TemplateNodeKind::DefinitionCall)
        {
            if (node.children.size() > std::numeric_limits<std::size_t>::max() - child_count)
                limitExceeded("canonical child count overflow");
            child_count += node.children.size();
        }
        if (node.enum_entries.size() > std::numeric_limits<std::size_t>::max() - enum_entry_count)
            limitExceeded("canonical Enum-entry count overflow");
        enum_entry_count += node.enum_entries.size();
    }

    std::pmr::vector<CheckerProof::CanonicalIRNodeView> nodes(scratch);
    std::pmr::vector<CheckerProof::CanonicalIRChildView> children(scratch);
    std::pmr::vector<CheckerProof::CanonicalIREnumEntryView> enum_entries(scratch);
    nodes.reserve(order.size());
    children.reserve(child_count);
    enum_entries.reserve(enum_entry_count);

    for (const TemplateNodeID source : order)
    {
        work.charge();
        const auto & node = definition.nodes[source];
        CheckerProof::CanonicalIRNodeView view{
            .kind = toCanonicalKind(node.kind),
            .atom = node.atom,
            .text = node.text,
            .parameter = node.parameter,
            .decrement = node.decrement,
            .unsigned_literal = node.unsigned_literal,
            .signed_literal = node.signed_literal,
            .boolean_literal = node.boolean_literal,
            .dependency_ordinal = node.dependency_ordinal,
            .children = {},
            .specialized_enum_width = toCanonicalEnumWidth(node.specialized_enum_width),
            .enum_entries = {},
            .field_kind = toCanonicalFieldKind(node.field_value.kind),
            .field_payload = fieldPayload(node.field_value),
            .field_name = node.field_value.name,
            .aggregate_nulls_action = toCanonicalNullsAction(node.aggregate_nulls_action),
        };
        const std::size_t children_begin = children.size();
        if (hasTemplateEdges(node.kind))
        {
            for (const auto & child : node.children)
            {
                work.charge();
                children.push_back({.reference = canonical_ids[child.reference], .label = child.label});
            }
        }
        else if (node.kind == TemplateNodeKind::DefinitionCall)
        {
            for (const auto & child : node.children)
            {
                work.charge();
                children.push_back({.reference = child.reference, .label = {}});
            }
        }
        if (children.size() != children_begin)
            view.children
                = std::span<const CheckerProof::CanonicalIRChildView>(children.data() + children_begin, children.size() - children_begin);

        const std::size_t entries_begin = enum_entries.size();
        for (const auto & entry : node.enum_entries)
            enum_entries.push_back({.name = entry.name, .value = entry.value});
        if (enum_entries.size() != entries_begin)
            view.enum_entries = std::span<const CheckerProof::CanonicalIREnumEntryView>(
                enum_entries.data() + entries_begin, enum_entries.size() - entries_begin);
        nodes.push_back(view);
    }

    const CheckerProof::CanonicalTemplateIRView canonical_ir{
        .formal_count = static_cast<UInt16>(definition.parameters.size()),
        .direct_dependency_count = static_cast<UInt16>(definition.dependencies.size()),
        .nodes = nodes,
    };
    const std::size_t encoded_size
        = CheckerProof::encodeCanonicalTemplateIR(canonical_ir, std::span<CheckerProof::Byte>{}, encoding_limits);
    String result(encoded_size, '\0');
    const std::size_t written = CheckerProof::encodeCanonicalTemplateIR(
        canonical_ir, std::span<CheckerProof::Byte>(reinterpret_cast<CheckerProof::Byte *>(result.data()), result.size()), encoding_limits);
    if (written != result.size())
        invariantViolation("canonical IR encoder returned an inconsistent size");
    return result;
}

TemplateCheckerCertificate checkOne(
    const DefinitionInput & definition,
    std::span<const std::size_t> dependency_targets,
    const std::vector<DefinitionInput> & definitions,
    TemplateChecker::BuiltInFamilyPredicateForTest is_registered_family,
    const Digest & compositional_closure_digest,
    UInt64 transitive_dependency_count,
    DefinitionWorkBudget & work,
    const TemplateCheckerLimits & limits,
    std::pmr::memory_resource * scratch)
{
    if (definition.nodes.empty() || definition.root >= definition.nodes.size())
        invalid("template root is absent or out of range");
    if (!producesType(definition.nodes[definition.root].kind))
        invalid("template root does not produce a type");
    if ((definition.semantic_capabilities & ~all_semantic_capabilities) != 0)
        invalid("semantic capability mask contains unknown bits");

    const DefinitionShape shape = validateTemplateDAG(definition, work, limits, scratch);
    const LocalValidationResult validation
        = validateDefinitionSemantics(definition, dependency_targets, definitions, is_registered_family, work, scratch);
    if (validation.has_self_call != (shape.self_call_occurrences != 0))
        invariantViolation("recursive proof counters disagree");

    const UInt16 expected_checker_abi = validation.has_self_call ? CHECKER_ABI_DECREASING_SELF : CHECKER_ABI_ACYCLIC;
    if (definition.checker_abi != expected_checker_abi || definition.checker_charge_abi != CHECKER_CHARGE_ABI
        || definition.policy_abi != POLICY_ABI || definition.function_registry_abi != FUNCTION_REGISTRY_ABI)
        invalid("checker, charge, policy, or function-registry ABI is unsupported");
    if (definition.policy_bearing == (definition.policy_semantic_hash == CheckerProof::empty_policy_semantic_hash))
        invalid("policy-bearing state disagrees with the canonical empty-policy marker");

    const CheckerProof::EncodingLimits encoding_limits = makeEncodingLimits(limits);
    String canonical_ir = encodeCanonicalIR(definition, work, encoding_limits, scratch);
    UInt64 canonical_bytes = checkedSize(canonical_ir.size(), "canonical IR bytes");
    if (canonical_bytes > limits.maximum_canonical_definition_bytes)
        limitExceeded("canonical definition bytes");

    std::pmr::vector<CheckerProof::FormalView> formals(scratch);
    formals.reserve(definition.parameters.size());
    for (const auto & parameter : definition.parameters)
        formals.push_back({.kind = toCheckerProofKind(parameter.kind), .normalized_name = parameter.normalized_name});

    std::pmr::vector<CheckerProof::DependencyIdentity> direct_dependencies(scratch);
    std::pmr::vector<CheckerProof::SemanticDependency> semantic_dependencies(scratch);
    direct_dependencies.reserve(definition.dependencies.size());
    semantic_dependencies.reserve(definition.dependencies.size());
    const CheckerProof::UUID database_uuid = toProofUUID(definition.identity.database_uuid);
    for (const auto & dependency : definition.dependencies)
    {
        direct_dependencies.push_back({
            .database_uuid = database_uuid,
            .type_uuid = toProofUUID(dependency.type_uuid),
            .revision = dependency.revision,
            .target_definition_hash = dependency.target_definition_hash,
        });
        semantic_dependencies.push_back({
            .type_uuid = toProofUUID(dependency.type_uuid),
            .revision = dependency.revision,
            .target_definition_hash = dependency.target_definition_hash,
        });
    }

    const CheckerProof::SemanticDefinitionView semantic_definition{
        .checker_abi = definition.checker_abi,
        .checker_charge_abi = definition.checker_charge_abi,
        .formals = formals,
        .has_decreasing_parameter = definition.decreasing_parameter.has_value(),
        .decreasing_parameter = definition.decreasing_parameter.value_or(0),
        .policy_bearing = definition.policy_bearing,
        .policy_abi = definition.policy_abi,
        .function_registry_abi = definition.function_registry_abi,
        .policy_semantic_hash = definition.policy_semantic_hash,
        .canonical_template_ir = canonical_ir,
        .direct_dependencies = semantic_dependencies,
    };
    const auto semantic_preimage = encodeTemporary(
        [&](std::span<CheckerProof::Byte> output)
        { return CheckerProof::semanticDigestPreimage(semantic_definition, output, encoding_limits); },
        scratch);
    const Digest semantic_digest = digestEncoded(semantic_preimage);
    const auto definition_hash_preimage = encodeTemporary(
        [&](std::span<CheckerProof::Byte> output)
        { return CheckerProof::definitionHashPreimage(semantic_digest, output, encoding_limits); },
        scratch);
    const Digest definition_hash = digestEncoded(definition_hash_preimage);
    const auto direct_dependency_preimage = encodeTemporary(
        [&](std::span<CheckerProof::Byte> output)
        {
            return CheckerProof::dependencySetDigestPreimage(
                CheckerProof::direct_dependency_set_domain, direct_dependencies, output, encoding_limits);
        },
        scratch);
    const Digest direct_dependency_digest = digestEncoded(direct_dependency_preimage);

    work.charge(checkedSize(formals.size() + direct_dependencies.size() + 1, "checker proof work"));
    const UInt64 checker_charge
        = CheckerProof::checkerChargeUnits(shape.logical_occurrences, formals.size(), direct_dependencies.size(), encoding_limits);
    const CheckerProof::CheckerProofView proof{
        .checker_abi = definition.checker_abi,
        .checker_charge_abi = definition.checker_charge_abi,
        .database_uuid = database_uuid,
        .type_uuid = toProofUUID(definition.identity.type_uuid),
        .revision = definition.identity.revision,
        .semantic_definition_digest = semantic_digest,
        .recursion_mode = validation.has_self_call ? CheckerProof::RecursionMode::DecreasingSelf : CheckerProof::RecursionMode::Acyclic,
        .policy_bearing = definition.policy_bearing,
        .has_measure = definition.decreasing_parameter.has_value(),
        .decreasing_parameter = definition.decreasing_parameter.value_or(0),
        .decreasing_parameter_kind = definition.decreasing_parameter
            ? toCheckerProofKind(definition.parameters[*definition.decreasing_parameter].kind)
            : CheckerProof::FormalKind::Type,
        .measure_rule = CheckerProof::MeasureRule::ZeroGuardNMinusOneWithReachableBase,
        .self_call_occurrence_count = shape.self_call_occurrences,
        .logical_node_count = shape.logical_occurrences,
        .maximum_template_depth = shape.maximum_node_depth - 1,
        .direct_dependency_count = direct_dependencies.size(),
        .direct_dependency_digest = direct_dependency_digest,
        .dependency_proof_mode = CheckerProof::DependencyProofMode::CompositionalClosure,
        .transitive_dependency_count = 0,
        .transitive_dependency_closure_digest = compositional_closure_digest,
        .checker_charge_units = checker_charge,
    };

    const std::size_t certificate_size = CheckerProof::encodeCheckerProof(proof, std::span<CheckerProof::Byte>{}, encoding_limits);
    addProspectively(
        canonical_bytes,
        checkedSize(certificate_size, "checker certificate bytes"),
        limits.maximum_canonical_definition_bytes,
        "canonical definition bytes");
    String encoded_certificate(certificate_size, '\0');
    const std::size_t written = CheckerProof::encodeCheckerProof(
        proof,
        std::span<CheckerProof::Byte>(reinterpret_cast<CheckerProof::Byte *>(encoded_certificate.data()), encoded_certificate.size()),
        encoding_limits);
    if (written != encoded_certificate.size())
        invariantViolation("checker-proof encoder returned an inconsistent size");
    const auto certificate_digest_preimage = encodeTemporary(
        [&](std::span<CheckerProof::Byte> output) { return CheckerProof::checkerProofDigestPreimage(proof, output, encoding_limits); },
        scratch);

    return {
        .canonical_template_ir = std::move(canonical_ir),
        .semantic_definition_digest = semantic_digest,
        .definition_hash = definition_hash,
        .compositional_dependency_closure_digest = compositional_closure_digest,
        .encoded_certificate = std::move(encoded_certificate),
        .certificate_digest = digestEncoded(certificate_digest_preimage),
        .charged_work = work.getCurrent(),
        .logical_node_count = shape.logical_occurrences,
        .maximum_template_depth = shape.maximum_node_depth - 1,
        .transitive_dependency_count = transitive_dependency_count,
    };
}

}

std::vector<Definition::Ptr>
TemplateChecker::checkAll(std::vector<DefinitionInput> inputs, const TemplateCheckerLimits & limits, TemplateCheckerStatistics * statistics)
{
    return checkAllImpl(std::move(inputs), isRegisteredBuiltInFamily, collidesWithRegisteredBuiltInFamilyOrAlias, limits, statistics);
}

std::vector<Definition::Ptr> TemplateChecker::checkAllWithBuiltInFamilyAuthorityForTest(
    std::vector<DefinitionInput> inputs,
    const BuiltInFamilyAuthorityForTest & built_in_authority,
    const TemplateCheckerLimits & limits,
    TemplateCheckerStatistics * statistics)
{
    return checkAllImpl(
        std::move(inputs),
        built_in_authority.is_registered_family,
        built_in_authority.collides_with_registered_family_or_alias,
        limits,
        statistics);
}

std::vector<Definition::Ptr> TemplateChecker::checkAllImpl(
    std::vector<DefinitionInput> inputs,
    BuiltInFamilyPredicateForTest is_registered_family,
    BuiltInFamilyPredicateForTest collides_with_registered_family_or_alias,
    const TemplateCheckerLimits & limits,
    TemplateCheckerStatistics * statistics)
{
    validateLimits(limits);
    if (!is_registered_family || !collides_with_registered_family_or_alias)
        invalid("built-in family authority is incomplete");
    const PreflightResult preflight = preflightInputs(inputs, limits);

    try
    {
        QuotaMemoryResource quota(limits.maximum_scratch_bytes);
        std::pmr::monotonic_buffer_resource scratch(&quota);
        CatalogWorkBudget catalog_work(limits.maximum_catalog_checker_work);

        std::pmr::unordered_set<DefinitionIdentity, DefinitionIdentityHash> identities(&scratch);
        std::pmr::unordered_set<std::string_view> names(&scratch);
        std::pmr::unordered_map<TargetKey, std::size_t, TargetKeyHash> targets_by_identity(&scratch);
        identities.reserve(inputs.size());
        names.reserve(inputs.size());
        targets_by_identity.reserve(inputs.size());

        UUID database_uuid = UUIDHelpers::Nil;
        for (std::size_t index = 0; index < inputs.size(); ++index)
        {
            const auto & input = inputs[index];
            if (input.identity.database_uuid == UUIDHelpers::Nil || input.identity.type_uuid == UUIDHelpers::Nil
                || input.identity.revision == 0)
                invalid("definition identity has a nil UUID or revision zero");
            if (index == 0)
                database_uuid = input.identity.database_uuid;
            else if (input.identity.database_uuid != database_uuid)
                invalid("bulk check crosses database authority");
            if (input.normalized_name.empty() || containsZero(input.normalized_name))
                invalid("definition name is empty or contains NUL");
            if (input.normalized_local_name.empty() && input.normalized_name.find('.') != String::npos)
                invalid("qualified definition name lacks a structured local identifier");
            const std::string_view local_name = input.normalized_local_name.empty() ? std::string_view(input.normalized_name)
                                                                                    : std::string_view(input.normalized_local_name);
            if (local_name.empty() || containsZero(local_name))
                invalid("definition local name is empty or contains NUL");
            if (collides_with_registered_family_or_alias(local_name))
                invalid("definition name collides with a registered built-in family or alias");
            if (!identities.emplace(input.identity).second)
                invalid("duplicate immutable definition identity");
            if (!names.emplace(local_name).second)
                invalid("duplicate normalized definition name");
            if (!targets_by_identity.emplace(TargetKey{input.identity.type_uuid, input.identity.revision}, index).second)
                invalid("duplicate type UUID and revision");
        }

        std::pmr::vector<std::size_t> dependency_offsets(&scratch);
        std::pmr::vector<std::size_t> dependency_targets(&scratch);
        dependency_offsets.reserve(inputs.size() + 1);
        dependency_offsets.push_back(0);
        for (std::size_t definition_index = 0; definition_index < inputs.size(); ++definition_index)
        {
            auto & input = inputs[definition_index];
            const std::size_t dependency_count = input.dependencies.size();
            std::pmr::vector<std::size_t> order(dependency_count, 0, &scratch);
            std::iota(order.begin(), order.end(), 0);
            std::sort(
                order.begin(),
                order.end(),
                [&](std::size_t lhs, std::size_t rhs) { return dependencyLess(input.dependencies[lhs], input.dependencies[rhs]); });
            for (std::size_t index = 1; index < order.size(); ++index)
            {
                const auto & prior = input.dependencies[order[index - 1]];
                const auto & current = input.dependencies[order[index]];
                if (prior.type_uuid == current.type_uuid && prior.revision == current.revision)
                    invalid("duplicate direct dependency identity");
            }

            std::pmr::vector<UInt16> remap(dependency_count, 0, &scratch);
            std::pmr::vector<UInt8> used(dependency_count, 0, &scratch);
            std::vector<DefinitionDependency> canonical_dependencies;
            canonical_dependencies.reserve(dependency_count);
            std::pmr::vector<std::size_t> canonical_targets(&scratch);
            canonical_targets.reserve(dependency_count);
            for (std::size_t canonical_ordinal = 0; canonical_ordinal < order.size(); ++canonical_ordinal)
            {
                const std::size_t original_ordinal = order[canonical_ordinal];
                if (canonical_ordinal > std::numeric_limits<UInt16>::max())
                    limitExceeded("dependency ordinal domain");
                remap[original_ordinal] = static_cast<UInt16>(canonical_ordinal);
                const auto & dependency = input.dependencies[original_ordinal];
                if (dependency.type_uuid == UUIDHelpers::Nil || dependency.revision == 0)
                    invalid("dependency identity has a nil UUID or revision zero");
                const auto target = targets_by_identity.find(TargetKey{dependency.type_uuid, dependency.revision});
                if (target == targets_by_identity.end())
                    invalid("dependency target is absent from the checked database set");
                if (target->second == definition_index)
                    invalid("self dependency must use a certified SelfCall node");
                canonical_dependencies.push_back(dependency);
                canonical_targets.push_back(target->second);
            }
            for (auto & node : input.nodes)
            {
                if (node.kind != TemplateNodeKind::DefinitionCall)
                    continue;
                if (node.dependency_ordinal >= dependency_count)
                    invalid("definition-call dependency ordinal is out of range");
                used[node.dependency_ordinal] = 1;
                node.dependency_ordinal = remap[node.dependency_ordinal];
            }
            if (std::find(used.begin(), used.end(), UInt8{0}) != used.end())
                invalid("declared direct dependency is unused");
            input.dependencies = std::move(canonical_dependencies);
            dependency_targets.insert(dependency_targets.end(), canonical_targets.begin(), canonical_targets.end());
            dependency_offsets.push_back(dependency_targets.size());
        }

        std::pmr::vector<UInt64> indegree(inputs.size(), 0, &scratch);
        catalog_work.charge(checkedSize(inputs.size(), "catalog definition count"));
        for (std::size_t source = 0; source < inputs.size(); ++source)
        {
            for (std::size_t cursor = dependency_offsets[source]; cursor < dependency_offsets[source + 1]; ++cursor)
            {
                catalog_work.charge();
                const std::size_t target = dependency_targets[cursor];
                if (indegree[target] == std::numeric_limits<UInt64>::max())
                    limitExceeded("catalog dependency indegree overflow");
                ++indegree[target];
            }
        }
        std::pmr::vector<std::size_t> topological_order(&scratch);
        topological_order.reserve(inputs.size());
        for (std::size_t index = 0; index < inputs.size(); ++index)
            if (indegree[index] == 0)
                topological_order.push_back(index);
        for (std::size_t cursor = 0; cursor < topological_order.size(); ++cursor)
        {
            const std::size_t source = topological_order[cursor];
            catalog_work.charge();
            for (std::size_t edge = dependency_offsets[source]; edge < dependency_offsets[source + 1]; ++edge)
            {
                catalog_work.charge();
                const std::size_t target = dependency_targets[edge];
                if (--indegree[target] == 0)
                    topological_order.push_back(target);
            }
        }
        if (topological_order.size() != inputs.size())
            invalid("multi-definition recursive SCC is forbidden");

        const CheckerProof::EncodingLimits encoding_limits = makeEncodingLimits(limits);
        const auto empty_closure_preimage = encodeTemporary(
            [&](std::span<CheckerProof::Byte> output)
            {
                return CheckerProof::compositionalClosureDigestPreimage(
                    std::span<const CheckerProof::CompositionalDependency>{}, output, encoding_limits);
            },
            &scratch);
        const Digest empty_closure = digestEncoded(empty_closure_preimage);

        std::pmr::vector<Digest> definition_hashes(inputs.size(), Digest{}, &scratch);
        std::pmr::vector<Digest> closure_digests(inputs.size(), Digest{}, &scratch);
        std::pmr::vector<std::size_t> closure_offsets(inputs.size(), 0, &scratch);
        std::pmr::vector<std::size_t> closure_counts(inputs.size(), 0, &scratch);
        std::pmr::vector<std::size_t> closure_entries(&scratch);
        std::pmr::vector<std::size_t> closure_seen(inputs.size(), 0, &scratch);
        std::pmr::vector<std::size_t> current_closure(&scratch);
        std::pmr::vector<UInt8> derived(inputs.size(), 0, &scratch);
        std::vector<TemplateCheckerCertificate> certificates(inputs.size());
        UInt64 catalog_canonical_bytes = 0;

        for (auto order_it = topological_order.rbegin(); order_it != topological_order.rend(); ++order_it)
        {
            const std::size_t definition_index = *order_it;
            auto & input = inputs[definition_index];
            const std::size_t begin = dependency_offsets[definition_index];
            const std::size_t end = dependency_offsets[definition_index + 1];
            catalog_work.charge(1 + checkedSize(end - begin, "closure dependency count"));

            current_closure.clear();
            const std::size_t seen_generation = definition_index + 1;
            const auto retain_closure_target = [&](std::size_t target)
            {
                catalog_work.charge();
                if (closure_seen[target] == seen_generation)
                    return;
                closure_seen[target] = seen_generation;
                if (current_closure.size() >= limits.maximum_transitive_dependencies)
                    limitExceeded("transitive dependency count");
                current_closure.push_back(target);
            };
            for (std::size_t cursor = begin; cursor < end; ++cursor)
            {
                const std::size_t target_index = dependency_targets[cursor];
                if (!derived[target_index])
                    invariantViolation("dependency-first order has no target dependency closure");
                retain_closure_target(target_index);
                const std::size_t target_offset = closure_offsets[target_index];
                const std::size_t target_count = closure_counts[target_index];
                for (std::size_t nested = 0; nested < target_count; ++nested)
                    retain_closure_target(closure_entries[target_offset + nested]);
            }
            closure_offsets[definition_index] = closure_entries.size();
            closure_counts[definition_index] = current_closure.size();
            closure_entries.insert(closure_entries.end(), current_closure.begin(), current_closure.end());

            if (begin == end)
            {
                closure_digests[definition_index] = empty_closure;
            }
            else
            {
                std::pmr::vector<CheckerProof::CompositionalDependency> closure_dependencies(&scratch);
                closure_dependencies.reserve(end - begin);
                for (std::size_t cursor = begin; cursor < end; ++cursor)
                {
                    const std::size_t target_index = dependency_targets[cursor];
                    const Digest & derived_hash = definition_hashes[target_index];
                    if (!derived[target_index])
                        invariantViolation("dependency-first order has no target definition hash");
                    auto & dependency = input.dependencies[cursor - begin];
                    if (!isZeroDigest(dependency.target_definition_hash) && dependency.target_definition_hash != derived_hash)
                        invalid("declared dependency hash does not match the derived target definition hash");
                    dependency.target_definition_hash = derived_hash;
                    closure_dependencies.push_back({
                        .identity = {
                            .database_uuid = toProofUUID(input.identity.database_uuid),
                            .type_uuid = toProofUUID(dependency.type_uuid),
                            .revision = dependency.revision,
                            .target_definition_hash = derived_hash,
                        },
                        .target_compositional_closure_digest = closure_digests[target_index],
                    });
                }
                const auto closure_preimage = encodeTemporary(
                    [&](std::span<CheckerProof::Byte> output)
                    { return CheckerProof::compositionalClosureDigestPreimage(closure_dependencies, output, encoding_limits); },
                    &scratch);
                closure_digests[definition_index] = digestEncoded(closure_preimage);
            }

            DefinitionWorkBudget definition_work(limits.maximum_checker_work, catalog_work);
            certificates[definition_index] = checkOne(
                input,
                std::span<const std::size_t>(dependency_targets.data(), dependency_targets.size()).subspan(begin, end - begin),
                inputs,
                is_registered_family,
                closure_digests[definition_index],
                closure_counts[definition_index],
                definition_work,
                limits,
                &scratch);
            definition_hashes[definition_index] = certificates[definition_index].definition_hash;
            derived[definition_index] = 1;
            UInt64 definition_canonical
                = checkedSize(certificates[definition_index].canonical_template_ir.size(), "canonical template IR bytes");
            addProspectively(
                definition_canonical,
                checkedSize(certificates[definition_index].encoded_certificate.size(), "checker certificate bytes"),
                limits.maximum_canonical_definition_bytes,
                "canonical definition bytes");
            addProspectively(
                catalog_canonical_bytes, definition_canonical, limits.maximum_canonical_catalog_bytes, "canonical catalog bytes");
        }

        std::vector<Definition::Ptr> result(inputs.size());
        for (std::size_t index = 0; index < inputs.size(); ++index)
            result[index]
                = Definition::Ptr(new Definition(std::move(inputs[index]), std::move(certificates[index])));

        TemplateCheckerStatistics successful_statistics{
            .accepted_input_bytes = preflight.accepted_input_bytes,
            .maximum_definition_input_bytes = preflight.maximum_definition_input_bytes,
            .checked_definitions = checkedSize(result.size(), "checked definition count"),
            .graph_edges = checkedSize(dependency_targets.size(), "catalog graph edge count"),
            .charged_work = catalog_work.getCurrent(),
            .canonical_bytes = catalog_canonical_bytes,
            .scratch_peak_bytes = quota.getPeakBytes(),
        };
        if (statistics)
            *statistics = successful_statistics;
        return result;
    }
    catch (const CheckerProof::EncodingError & error)
    {
        if (error.code == CheckerProof::EncodingError::Code::LimitExceeded)
            limitExceeded(error.what());
        invalid(error.what());
    }
    catch (const std::bad_alloc &)
    {
        limitExceeded("allocation failed within an admitted bound");
    }
    catch (const std::length_error &)
    {
        limitExceeded("container length exceeds the host allocation domain");
    }
}

}
