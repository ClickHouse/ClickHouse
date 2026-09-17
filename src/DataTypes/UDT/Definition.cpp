#include <DataTypes/UDT/Definition.h>

#include <DataTypes/UDT/CanonicalHash.h>

#include <Core/Field.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <iterator>
#include <limits>
#include <type_traits>
#include <utility>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int BAD_ARGUMENTS;
extern const int TOO_MANY_BYTES;
}

namespace DB::UDT
{
namespace
{

template <typename Value>
String encodeLittleEndian(Value value)
{
    WriteBufferFromOwnString out;
    writeBinaryLittleEndian(value, out);
    return out.str();
}

template <typename Decimal>
String encodeDecimal(const DecimalField<Decimal> & value)
{
    WriteBufferFromOwnString out;
    writeBinaryLittleEndian(value.getValue(), out);
    writeBinaryLittleEndian(value.getScale(), out);
    return out.str();
}

[[noreturn]] void unsupportedField(Field::Types::Which type)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "User-defined type aggregate parameter is not supported by FieldBinaryEncoding: {}",
        fieldTypeToString(type));
}

CanonicalFieldKind canonicalNullKind(const Field & field)
{
    chassert(field.getType() == Field::Types::Null);
    return Field::dispatch(
        [](const auto & value) -> CanonicalFieldKind
        {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, Null>)
            {
                if (value.isNull())
                    return CanonicalFieldKind::Null;
                if (value.isNegativeInfinity())
                    return CanonicalFieldKind::NegativeInfinity;
                if (value.isPositiveInfinity())
                    return CanonicalFieldKind::PositiveInfinity;
            }
            unsupportedField(Field::Types::Null);
        },
        field);
}

[[noreturn]] void fieldLimit(std::string_view message)
{
    throw Exception(ErrorCodes::TOO_MANY_BYTES, "User-defined type aggregate parameter {}", message);
}

void charge(UInt64 addition, UInt64 & total, UInt64 maximum, std::string_view what)
{
    if (addition > maximum || total > maximum - addition)
        fieldLimit(what);
    total += addition;
}

UInt64 fieldSize(std::size_t value, std::string_view what)
{
    if (!std::in_range<UInt64>(value))
        fieldLimit(what);
    return static_cast<UInt64>(value);
}

struct CanonicalFieldPreflight
{
    UInt64 node_count = 0;
    UInt64 edge_count = 0;
    UInt64 entry_count = 0;
    UInt64 literal_bytes = 0;
};

void preflightCanonicalField(const Field & field, UInt64 depth, const CanonicalFieldValueLimits & limits, CanonicalFieldPreflight & state)
{
    /// This bound is part of the canonical Field contract and keeps
    /// this allocation-free recursive traversal bounded independently of a
    /// widened caller limit.
    constexpr UInt64 implementation_maximum_depth = 256;
    if (depth > limits.maximum_depth || depth > implementation_maximum_depth)
        fieldLimit("depth exceeds its limit");
    charge(1, state.node_count, limits.maximum_nodes, "node count exceeds its limit");

    const auto charge_literal = [&](std::size_t size)
    {
        charge(
            fieldSize(size, "literal byte count exceeds the UInt64 domain"),
            state.literal_bytes,
            limits.maximum_literal_bytes,
            "literal bytes exceed their limit");
    };
    const auto charge_children = [&](std::size_t child_count, std::size_t entry_count)
    {
        charge(
            fieldSize(child_count, "edge count exceeds the UInt64 domain"),
            state.edge_count,
            limits.maximum_edges,
            "edge count exceeds its limit");
        charge(
            fieldSize(entry_count, "entry count exceeds the UInt64 domain"),
            state.entry_count,
            limits.maximum_entries,
            "entry count exceeds its limit");
    };
    const auto child_depth = [&]
    {
        if (depth == std::numeric_limits<UInt64>::max())
            fieldLimit("depth exceeds its limit");
        return depth + 1;
    };

    switch (field.getType())
    {
        case Field::Types::Null: static_cast<void>(canonicalNullKind(field)); break;
        case Field::Types::UInt64:
        case Field::Types::Int64:
        case Field::Types::Float64: charge_literal(sizeof(UInt64)); break;
        case Field::Types::String: charge_literal(field.safeGet<String>().size()); break;
        case Field::Types::Bool: charge_literal(1); break;
        case Field::Types::UInt128:
        case Field::Types::Int128:
        case Field::Types::UUID:
        case Field::Types::IPv6: charge_literal(16); break;
        case Field::Types::UInt256:
        case Field::Types::Int256: charge_literal(32); break;
        case Field::Types::Decimal32: charge_literal(8); break;
        case Field::Types::Decimal64: charge_literal(12); break;
        case Field::Types::Decimal128: charge_literal(20); break;
        case Field::Types::Decimal256: charge_literal(36); break;
        case Field::Types::IPv4: charge_literal(4); break;
        case Field::Types::Array: {
            const auto & values = field.safeGet<Array>();
            charge_children(values.size(), values.size());
            const UInt64 next_depth = child_depth();
            for (const auto & child : values)
                preflightCanonicalField(child, next_depth, limits, state);
            break;
        }
        case Field::Types::Tuple: {
            const auto & values = field.safeGet<Tuple>();
            charge_children(values.size(), values.size());
            const UInt64 next_depth = child_depth();
            for (const auto & child : values)
                preflightCanonicalField(child, next_depth, limits, state);
            break;
        }
        case Field::Types::Map: {
            const auto & values = field.safeGet<Map>();
            if (values.size() > std::numeric_limits<std::size_t>::max() / 2)
                fieldLimit("Map entry count overflows size_t");
            for (const auto & entry : values)
            {
                if (entry.getType() != Field::Types::Tuple || entry.safeGet<Tuple>().size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "User-defined type aggregate Map parameter has a malformed entry");
            }
            charge_children(values.size() * 2, values.size());
            const UInt64 next_depth = child_depth();
            for (const auto & entry : values)
            {
                const auto & pair = entry.safeGet<Tuple>();
                preflightCanonicalField(pair[0], next_depth, limits, state);
                preflightCanonicalField(pair[1], next_depth, limits, state);
            }
            break;
        }
        case Field::Types::Object: {
            const auto & values = field.safeGet<Object>();
            charge_children(values.size(), values.size());
            const UInt64 next_depth = child_depth();
            for (const auto & [key, child] : values)
            {
                charge_literal(key.size());
                preflightCanonicalField(child, next_depth, limits, state);
            }
            break;
        }
        case Field::Types::AggregateFunctionState: {
            const auto & value = field.safeGet<AggregateFunctionStateData>();
            charge_literal(value.data.size());
            charge_literal(value.name.size());
            break;
        }
        case Field::Types::CustomType: unsupportedField(field.getType());
    }
}

CanonicalFieldValue fieldValue(CanonicalFieldKind kind, String payload = {}, String name = {})
{
    CanonicalFieldValue result;
    result.kind = kind;
    result.payload = std::move(payload);
    result.name = std::move(name);
    return result;
}

}

CanonicalFieldValue CanonicalFieldValue::fromField(const Field & field)
{
    switch (field.getType())
    {
        case Field::Types::Null: return fieldValue(canonicalNullKind(field));
        case Field::Types::UInt64: return fieldValue(CanonicalFieldKind::UInt64, encodeLittleEndian(field.safeGet<UInt64>()));
        case Field::Types::Int64: return fieldValue(CanonicalFieldKind::Int64, encodeLittleEndian(field.safeGet<Int64>()));
        case Field::Types::Float64: return fieldValue(CanonicalFieldKind::Float64, encodeLittleEndian(field.safeGet<Float64>()));
        case Field::Types::String: return fieldValue(CanonicalFieldKind::String, field.safeGet<String>());
        case Field::Types::Bool: return fieldValue(CanonicalFieldKind::Bool, String(1, static_cast<char>(field.safeGet<bool>() ? 1 : 0)));
        case Field::Types::UInt128: return fieldValue(CanonicalFieldKind::UInt128, encodeLittleEndian(field.safeGet<UInt128>()));
        case Field::Types::Int128: return fieldValue(CanonicalFieldKind::Int128, encodeLittleEndian(field.safeGet<Int128>()));
        case Field::Types::UInt256: return fieldValue(CanonicalFieldKind::UInt256, encodeLittleEndian(field.safeGet<UInt256>()));
        case Field::Types::Int256: return fieldValue(CanonicalFieldKind::Int256, encodeLittleEndian(field.safeGet<Int256>()));
        case Field::Types::Decimal32:
            return fieldValue(CanonicalFieldKind::Decimal32, encodeDecimal(field.safeGet<DecimalField<Decimal32>>()));
        case Field::Types::Decimal64:
            return fieldValue(CanonicalFieldKind::Decimal64, encodeDecimal(field.safeGet<DecimalField<Decimal64>>()));
        case Field::Types::Decimal128:
            return fieldValue(CanonicalFieldKind::Decimal128, encodeDecimal(field.safeGet<DecimalField<Decimal128>>()));
        case Field::Types::Decimal256:
            return fieldValue(CanonicalFieldKind::Decimal256, encodeDecimal(field.safeGet<DecimalField<Decimal256>>()));
        case Field::Types::UUID: {
            const auto bytes = uuidToCanonicalBytes(field.safeGet<UUID>());
            return fieldValue(CanonicalFieldKind::UUID, String(reinterpret_cast<const char *>(bytes.data()), bytes.size()));
        }
        case Field::Types::IPv4: return fieldValue(CanonicalFieldKind::IPv4, encodeLittleEndian(field.safeGet<IPv4>().toUnderType()));
        case Field::Types::IPv6: return fieldValue(CanonicalFieldKind::IPv6, encodeLittleEndian(field.safeGet<IPv6>().toUnderType()));
        case Field::Types::Array: return fieldValue(CanonicalFieldKind::Array);
        case Field::Types::Tuple: return fieldValue(CanonicalFieldKind::Tuple);
        case Field::Types::Map: return fieldValue(CanonicalFieldKind::Map);
        case Field::Types::Object: return fieldValue(CanonicalFieldKind::Object);
        case Field::Types::AggregateFunctionState: {
            const auto & value = field.safeGet<AggregateFunctionStateData>();
            return fieldValue(CanonicalFieldKind::AggregateFunctionState, value.data, value.name);
        }
        case Field::Types::CustomType: unsupportedField(field.getType());
    }
    unsupportedField(field.getType());
}

bool Definition::hasSameCheckedSemantics(const Definition & other) const noexcept
{
    return identity == other.identity && parameters == other.parameters && decreasing_parameter == other.decreasing_parameter
        && nodes == other.nodes && root == other.root && policy_bearing == other.policy_bearing
        && semantic_capabilities == other.semantic_capabilities && checker_abi == other.checker_abi
        && checker_charge_abi == other.checker_charge_abi && policy_abi == other.policy_abi
        && function_registry_abi == other.function_registry_abi && policy_semantic_hash == other.policy_semantic_hash
        && dependencies == other.dependencies && certificate == other.certificate;
}

std::vector<TemplateNodeID> appendCanonicalFieldValues(
    std::span<const Field> fields, std::vector<TemplateNode> & nodes, CanonicalFieldValueLimits limits)
{
    struct PendingField
    {
        const Field * field = nullptr;
        UInt64 depth = 0;
    };

    if (nodes.size() > std::numeric_limits<TemplateNodeID>::max())
        fieldLimit("node count exceeds the TemplateNodeID domain");

    CanonicalFieldPreflight preflight;
    for (const auto & field : fields)
        preflightCanonicalField(field, 1, limits, preflight);
    if (preflight.node_count > static_cast<UInt64>(std::numeric_limits<TemplateNodeID>::max()) + 1 - nodes.size())
        fieldLimit("node count exceeds the TemplateNodeID domain");

    std::vector<TemplateNode> additions(static_cast<std::size_t>(preflight.node_count));
    std::vector<PendingField> pending;
    pending.reserve(additions.size());
    std::vector<TemplateNodeID> roots;
    roots.reserve(fields.size());
    for (size_t index = 0; index < fields.size(); ++index)
    {
        pending.push_back({.field = &fields[index], .depth = 1});
        roots.push_back(static_cast<TemplateNodeID>(nodes.size() + index));
    }

    for (size_t current = 0; current < pending.size(); ++current)
    {
        const auto & pending_field = pending[current];
        const Field & field = *pending_field.field;

        auto & output = additions[current];
        output.kind = TemplateNodeKind::FieldValue;
        output.field_value = CanonicalFieldValue::fromField(field);

        size_t child_count = 0;
        switch (field.getType())
        {
            case Field::Types::Array: child_count = field.safeGet<Array>().size(); break;
            case Field::Types::Tuple: child_count = field.safeGet<Tuple>().size(); break;
            case Field::Types::Map: {
                const auto & value = field.safeGet<Map>();
                if (value.size() > std::numeric_limits<size_t>::max() / 2)
                    fieldLimit("Map entry count overflows size_t");
                child_count = value.size() * 2;
                break;
            }
            case Field::Types::Object: child_count = field.safeGet<Object>().size(); break;
            default: break;
        }

        if (child_count == 0)
            continue;

        output.children.reserve(child_count);
        const UInt64 child_depth = pending_field.depth + 1;
        const auto append_child = [&](const Field & child, String label = {})
        {
            const auto reference = static_cast<TemplateNodeID>(nodes.size() + pending.size());
            output.children.push_back({.reference = reference, .label = std::move(label)});
            pending.push_back({.field = &child, .depth = child_depth});
        };

        switch (field.getType())
        {
            case Field::Types::Array:
                for (const auto & child : field.safeGet<Array>())
                    append_child(child);
                break;
            case Field::Types::Tuple:
                for (const auto & child : field.safeGet<Tuple>())
                    append_child(child);
                break;
            case Field::Types::Map:
                for (const auto & entry : field.safeGet<Map>())
                {
                    if (entry.getType() != Field::Types::Tuple || entry.safeGet<Tuple>().size() != 2)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "User-defined type aggregate Map parameter has a malformed entry");
                    const auto & pair = entry.safeGet<Tuple>();
                    append_child(pair[0]);
                    append_child(pair[1]);
                }
                break;
            case Field::Types::Object:
                for (const auto & [key, child] : field.safeGet<Object>())
                    append_child(child, key);
                break;
            default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-composite Field unexpectedly has children");
        }
    }

    if (pending.size() != additions.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Canonical Field preflight and construction node counts diverged");

    nodes.reserve(nodes.size() + additions.size());
    nodes.insert(nodes.end(), std::make_move_iterator(additions.begin()), std::make_move_iterator(additions.end()));
    return roots;
}

CheckerProof::FormalKind toCheckerProofKind(ParameterKind kind)
{
    switch (kind)
    {
        case ParameterKind::Type: return CheckerProof::FormalKind::Type;
        case ParameterKind::Bool: return CheckerProof::FormalKind::Bool;
        case ParameterKind::UInt8: return CheckerProof::FormalKind::UInt8;
        case ParameterKind::UInt16: return CheckerProof::FormalKind::UInt16;
        case ParameterKind::UInt32: return CheckerProof::FormalKind::UInt32;
        case ParameterKind::UInt64: return CheckerProof::FormalKind::UInt64;
        case ParameterKind::Int8: return CheckerProof::FormalKind::Int8;
        case ParameterKind::Int16: return CheckerProof::FormalKind::Int16;
        case ParameterKind::Int32: return CheckerProof::FormalKind::Int32;
        case ParameterKind::Int64: return CheckerProof::FormalKind::Int64;
        case ParameterKind::String: return CheckerProof::FormalKind::String;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown user-defined type parameter kind: {}", static_cast<UInt64>(kind));
}

bool isUnsignedIntegerParameter(ParameterKind kind) noexcept
{
    return kind == ParameterKind::UInt8 || kind == ParameterKind::UInt16 || kind == ParameterKind::UInt32 || kind == ParameterKind::UInt64;
}

bool isSignedIntegerParameter(ParameterKind kind) noexcept
{
    return kind == ParameterKind::Int8 || kind == ParameterKind::Int16 || kind == ParameterKind::Int32 || kind == ParameterKind::Int64;
}

bool isIntegerParameter(ParameterKind kind) noexcept
{
    return isUnsignedIntegerParameter(kind) || isSignedIntegerParameter(kind);
}

Definition::Definition(DefinitionInput input, TemplateCheckerCertificate certificate_)
    : identity(input.identity)
    , normalized_local_name(input.normalized_local_name.empty() ? input.normalized_name : std::move(input.normalized_local_name))
    , normalized_name(std::move(input.normalized_name))
    , parameters(std::move(input.parameters))
    , decreasing_parameter(input.decreasing_parameter)
    , nodes(std::move(input.nodes))
    , root(input.root)
    , policy_bearing(input.policy_bearing)
    , semantic_capabilities(input.semantic_capabilities)
    , checker_abi(input.checker_abi)
    , checker_charge_abi(input.checker_charge_abi)
    , policy_abi(input.policy_abi)
    , function_registry_abi(input.function_registry_abi)
    , policy_semantic_hash(input.policy_semantic_hash)
    , dependencies(std::move(input.dependencies))
    , certificate(std::move(certificate_))
{
}

std::optional<UInt64> tryCountLogicalRetainedDefinitionBytes(const Definition & definition, UInt64 maximum_bytes) noexcept
{
    UInt64 result = 0;
    const auto charge = [&](UInt64 amount)
    {
        if (amount > maximum_bytes || result > maximum_bytes - amount)
            return false;
        result += amount;
        return true;
    };
    const auto charge_size = [&](std::size_t size)
    {
        if (!std::in_range<UInt64>(size))
            return false;
        return charge(static_cast<UInt64>(size));
    };
    const auto charge_string = [&](std::string_view value) { return charge_size(value.size()); };
    const auto charge_vector = [&]<typename T>(const std::vector<T> & values)
    {
        if (!std::in_range<UInt64>(values.size()))
            return false;
        const UInt64 count = static_cast<UInt64>(values.size());
        if (count != 0 && sizeof(T) > maximum_bytes / count)
            return false;
        return charge(count * sizeof(T));
    };

    if (!charge(sizeof(Definition)) || !charge_string(definition.getNormalizedName())
        || !charge_string(definition.getNormalizedLocalName()) || !charge_vector(definition.getParameters()))
        return std::nullopt;
    for (const auto & parameter : definition.getParameters())
        if (!charge_string(parameter.normalized_name))
            return std::nullopt;

    if (!charge_vector(definition.getNodes()))
        return std::nullopt;
    for (const auto & node : definition.getNodes())
    {
        if (!charge_string(node.atom) || !charge_string(node.text) || !charge_string(node.field_value.payload)
            || !charge_string(node.field_value.name) || !charge_vector(node.enum_entries))
            return std::nullopt;
        for (const auto & entry : node.enum_entries)
            if (!charge_string(entry.name))
                return std::nullopt;
        if (!charge_vector(node.children))
            return std::nullopt;
        for (const auto & child : node.children)
            if (!charge_string(child.label))
                return std::nullopt;
    }

    if (!charge_vector(definition.getDependencies()))
        return std::nullopt;
    const auto & certificate = definition.getCertificate();
    if (!charge_string(certificate.canonical_template_ir) || !charge_string(certificate.encoded_certificate))
        return std::nullopt;
    return result;
}

}
