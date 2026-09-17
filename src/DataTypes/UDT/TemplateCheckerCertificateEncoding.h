#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace DB::UDT::CheckerProof
{

using Byte = std::uint8_t;
using Digest = std::array<Byte, 32>;
using UUID = std::array<Byte, 16>;

/// SHA-256("ClickHouse UDT empty policy V1\0\0"). The first NUL terminates
/// the domain and the second is the canonical zero-length payload frame.
/// Keeping this marker here prevents adapters from assigning different
/// semantics to an otherwise identical non-policy-bearing definition.
inline constexpr Digest empty_policy_semantic_hash{
    0x0f, 0x6d, 0xb2, 0x6b, 0x82, 0xc7, 0xaf, 0xa6, 0xe3, 0x56, 0x3a, 0x4d, 0x50, 0x50, 0x8f, 0x42,
    0x2d, 0x92, 0x5e, 0x1c, 0xf4, 0x2e, 0xd7, 0x17, 0x91, 0x77, 0x3a, 0x42, 0xec, 0x6a, 0x60, 0xa4,
};

inline constexpr std::uint16_t encoding_version = 2;
inline constexpr std::uint16_t proof_version = 2;
inline constexpr std::uint16_t legacy_canonical_ir_version = 2;

/// V2 is immutable. V3 is a separate, self-domain-separated canonical IR and
/// does not change any V2 tag, function, domain, or byte sequence below.
inline constexpr std::uint16_t canonical_ir_version = 3;
inline constexpr std::string_view canonical_template_ir_domain = "ClickHouse UDT canonical template IR V3";

inline constexpr std::string_view semantic_definition_domain = "ClickHouse UDT semantic definition V2";
inline constexpr std::string_view definition_hash_domain = "ClickHouse UDT definition hash V2";
inline constexpr std::string_view dependency_set_domain = "ClickHouse UDT dependency set V2";
inline constexpr std::string_view direct_dependency_set_domain = "ClickHouse UDT direct dependency set V2";
inline constexpr std::string_view transitive_dependency_set_domain = "ClickHouse UDT transitive dependency set V2";
inline constexpr std::string_view compositional_dependency_closure_domain = "ClickHouse UDT compositional dependency closure V2";
inline constexpr std::string_view checker_proof_domain = "ClickHouse UDT checker proof V2";

/// Stable wire values. Adapters must not serialize their local enum ordinals.
enum class FormalKind : Byte
{
    Type = 1,
    Bool = 2,
    UInt8 = 3,
    UInt16 = 4,
    UInt32 = 5,
    UInt64 = 6,
    Int8 = 7,
    Int16 = 8,
    Int32 = 9,
    Int64 = 10,
    String = 11,
};

enum class RecursionMode : Byte
{
    Acyclic = 0,
    DecreasingSelf = 1,
};

/// This rule asserts all three obligations: a zero guard, N - 1 on every
/// recursive edge, and a reachable non-recursive zero branch.
enum class MeasureRule : Byte
{
    ZeroGuardNMinusOneWithReachableBase = 1,
};

enum class DependencyProofMode : Byte
{
    /// One reverse-topological pass. The digest binds each sorted direct
    /// identity to the already-computed commitment of its target.
    CompositionalClosure = 1,
    /// The digest and count describe the sorted unique transitive identity set.
    ExactUniqueClosure = 2,
};

enum class LegacyCanonicalIRNodeKind : Byte
{
    BuiltIn = 1,
    TypeFormal = 2,
    ValueFormal = 3,
    UnsignedLiteral = 4,
    BooleanLiteral = 5,
    TypeIfZero = 6,
    SelfCall = 7,
    ExternalCall = 8,
};

/// Stable V3 wire values. Values 1..8 intentionally retain the meaning of the
/// corresponding V2 nodes; V3 remains a separately versioned wire format.
enum class CanonicalIRNodeKind : Byte
{
    BuiltIn = 1,
    TypeFormal = 2,
    ValueFormal = 3,
    UnsignedLiteral = 4,
    BooleanLiteral = 5,
    TypeIfZero = 6,
    SelfCall = 7,
    ExternalCall = 8,
    SignedLiteral = 9,
    StringLiteral = 10,
    Identifier = 11,
    SpecializedEnum = 12,
    FieldValue = 13,
    AggregateFunction = 14,
    DynamicSetting = 15,
    ObjectSetting = 16,
    ObjectTypedPath = 17,
    ObjectSkipPath = 18,
    ObjectSkipRegexp = 19,
};

/// Stable Field tags used only by FieldValue nodes. Leaf payloads are a
/// value-preserving canonical byte representation, not formatted SQL and not
/// an arithmetic normalization. In particular Float64 stores its exact IEEE
/// bits, including signed zero and NaN payloads. Array/Tuple/Map/Object values
/// use structural child edges; they are never hidden inside an opaque frame.
enum class CanonicalFieldKind : Byte
{
    None = 0,
    Null = 1,
    UInt64 = 2,
    Int64 = 3,
    Float64 = 4,
    String = 5,
    Bool = 6,
    UInt128 = 7,
    Int128 = 8,
    UInt256 = 9,
    Int256 = 10,
    Decimal32 = 11,
    Decimal64 = 12,
    Decimal128 = 13,
    Decimal256 = 14,
    UUID = 15,
    IPv4 = 16,
    IPv6 = 17,
    NegativeInfinity = 18,
    PositiveInfinity = 19,
    Array = 20,
    Tuple = 21,
    Map = 22,
    Object = 23,
    AggregateFunctionState = 24,
};

/// Stable wire values; never serialize DB::NullsAction's compiler ordinal.
enum class AggregateFunctionNullsAction : Byte
{
    Empty = 0,
    RespectNulls = 1,
    IgnoreNulls = 2,
};

enum class SpecializedEnumWidth : Byte
{
    None = 0,
    Enum8 = 1,
    Enum16 = 2,
};

struct CanonicalIRChildView
{
    /// Canonical node ID for a built-in type or TypeIfZero; caller-formal ordinal for
    /// ExternalCall.
    std::uint32_t reference = 0;
    /// Only built-in type edges may carry a physical field label.
    std::string_view label;
};

struct LegacyCanonicalIRNodeView
{
    LegacyCanonicalIRNodeKind kind = LegacyCanonicalIRNodeKind::BuiltIn;
    /// Nonempty only for a built-in type.
    std::string_view atom;
    /// Formal ordinal for TypeFormal/ValueFormal/TypeIfZero/SelfCall.
    std::uint16_t parameter = 0;
    /// Exactly one only for SelfCall.
    std::uint64_t decrement = 0;
    /// Used only for UnsignedLiteral.
    std::uint64_t unsigned_literal = 0;
    /// Used only for BooleanLiteral.
    bool boolean_literal = false;
    /// Direct-dependency ordinal for ExternalCall.
    std::uint16_t dependency_ordinal = 0;
    std::span<const CanonicalIRChildView> children;
};

struct LegacyCanonicalTemplateIRView
{
    std::uint16_t formal_count = 0;
    std::uint16_t direct_dependency_count = 0;
    /// Node zero is the root. Template edges use a strict parent-before-child
    /// topological order, so every referenced node ID is greater than its
    /// source ID. Shared DAG nodes are allowed.
    std::span<const LegacyCanonicalIRNodeView> nodes;
};

struct CanonicalIREnumEntryView
{
    /// Raw, binary-safe Enum label after parser literal decoding. Empty labels
    /// are legal in ClickHouse.
    std::string_view name;
    std::int64_t value = 0;
};

struct CanonicalIRNodeView
{
    CanonicalIRNodeKind kind = CanonicalIRNodeKind::BuiltIn;
    /// Nonempty only for a built-in type. This is a canonical family name, not SQL.
    std::string_view atom;
    /// Raw string payload for StringLiteral, or a normalized identifier for
    /// Identifier. No source-SQL/opaque-expression node exists in V3.
    std::string_view text;
    /// Formal ordinal for TypeFormal/ValueFormal/TypeIfZero/SelfCall.
    std::uint16_t parameter = 0;
    /// Exactly one only for SelfCall.
    std::uint64_t decrement = 0;
    std::uint64_t unsigned_literal = 0;
    std::int64_t signed_literal = 0;
    bool boolean_literal = false;
    /// Direct-dependency ordinal for ExternalCall.
    std::uint16_t dependency_ordinal = 0;
    std::span<const CanonicalIRChildView> children;
    SpecializedEnumWidth specialized_enum_width = SpecializedEnumWidth::None;
    /// Nonempty only for SpecializedEnum. Entries are stored in strictly
    /// increasing numeric order, matching the canonical DataTypeEnum order.
    std::span<const CanonicalIREnumEntryView> enum_entries;
    CanonicalFieldKind field_kind = CanonicalFieldKind::None;
    /// Little-endian fixed-width payload for numeric/decimal/IP values,
    /// RFC textual-order bytes for UUID, raw bytes for String, or aggregate
    /// state data. Null, infinities, and composites have an empty payload;
    /// Decimal payloads end with a u32le scale.
    std::span<const Byte> field_payload;
    /// Raw aggregate-state type name. Empty for every other Field kind.
    std::string_view field_name;
    AggregateFunctionNullsAction aggregate_nulls_action = AggregateFunctionNullsAction::Empty;
};

struct CanonicalTemplateIRView
{
    std::uint16_t formal_count = 0;
    std::uint16_t direct_dependency_count = 0;
    /// Node zero is the root. Template edges use a strict parent-before-child
    /// order and dense first-discovery IDs, exactly as in V2.
    std::span<const CanonicalIRNodeView> nodes;
};

struct FormalView
{
    FormalKind kind = FormalKind::Type;
    std::string_view normalized_name;
};

struct DependencyIdentity
{
    UUID database_uuid{};
    UUID type_uuid{};
    std::uint64_t revision = 0;
    Digest target_definition_hash{};

    bool operator==(const DependencyIdentity &) const = default;
};

/// Semantic dependencies are same-authority references. Omitting the owning
/// database UUID keeps definition hashes stable when a database restore remaps
/// that UUID; the identity-specific proof binds the database separately.
struct SemanticDependency
{
    UUID type_uuid{};
    std::uint64_t revision = 0;
    Digest target_definition_hash{};

    bool operator==(const SemanticDependency &) const = default;
};

struct CompositionalDependency
{
    DependencyIdentity identity;
    Digest target_compositional_closure_digest{};
};

struct SemanticDefinitionView
{
    std::uint16_t checker_abi = 0;
    std::uint16_t checker_charge_abi = 0;
    std::span<const FormalView> formals;
    bool has_decreasing_parameter = false;
    std::uint16_t decreasing_parameter = 0;
    bool policy_bearing = false;
    std::uint16_t policy_abi = 0;
    std::uint16_t function_registry_abi = 0;
    Digest policy_semantic_hash{};
    /// Name-free, canonical checker IR. It is binary-safe and length framed.
    std::string_view canonical_template_ir;
    /// Direct dependencies in strictly increasing canonical identity order.
    std::span<const SemanticDependency> direct_dependencies;
};

struct CheckerProofView
{
    std::uint16_t checker_abi = 0;
    std::uint16_t checker_charge_abi = 0;
    UUID database_uuid{};
    UUID type_uuid{};
    std::uint64_t revision = 0;
    Digest semantic_definition_digest{};
    RecursionMode recursion_mode = RecursionMode::Acyclic;
    bool policy_bearing = false;
    bool has_measure = false;
    std::uint16_t decreasing_parameter = 0;
    FormalKind decreasing_parameter_kind = FormalKind::Type;
    MeasureRule measure_rule = MeasureRule::ZeroGuardNMinusOneWithReachableBase;
    /// Reachable serialized incoming SELF_CALL edges/occurrences, not the
    /// number of distinct interned IR nodes.
    std::uint64_t self_call_occurrence_count = 0;
    std::uint64_t logical_node_count = 0;
    /// Maximum root-to-node edge count. A one-node template therefore has
    /// depth zero.
    std::uint64_t maximum_template_depth = 0;
    std::uint64_t direct_dependency_count = 0;
    Digest direct_dependency_digest{};
    DependencyProofMode dependency_proof_mode = DependencyProofMode::CompositionalClosure;
    /// Zero is the required “not materialized” sentinel in compositional mode.
    std::uint64_t transitive_dependency_count = 0;
    Digest transitive_dependency_closure_digest{};
    std::uint64_t checker_charge_units = 0;
};

struct EncodingLimits
{
    std::uint64_t maximum_output_bytes = 16ULL << 20;
    std::uint64_t maximum_formals = 64;
    std::uint64_t maximum_formal_name_bytes = 1ULL << 10;
    std::uint64_t maximum_template_ir_bytes = 256ULL << 10;
    std::uint64_t maximum_direct_dependencies = 256;
    std::uint64_t maximum_transitive_dependencies = 1'024;
    std::uint64_t maximum_logical_nodes = 1ULL << 20;
    std::uint64_t maximum_template_depth = 1ULL << 16;
    std::uint64_t maximum_template_edges = 1ULL << 20;
    std::uint64_t maximum_ir_atom_bytes = 64ULL << 10;
    std::uint64_t maximum_ir_literal_bytes = 256ULL << 10;
    std::uint64_t maximum_ir_identifier_bytes = 1ULL << 10;
    std::uint64_t maximum_ir_enum_entries = 1ULL << 16;
    std::uint64_t maximum_checker_charge_units = 1ULL << 32;
};

class EncodingError final : public std::runtime_error
{
public:
    enum class Code : Byte
    {
        InvalidValue,
        LimitExceeded,
        OutputSizeMismatch,
    };

    EncodingError(Code code_, std::string_view message)
        : std::runtime_error(std::string(message))
        , code(code_)
    {
    }

    Code code;
};

namespace detail
{

[[noreturn]] inline void invalid(std::string_view message)
{
    throw EncodingError(EncodingError::Code::InvalidValue, message);
}

[[noreturn]] inline void limit(std::string_view message)
{
    throw EncodingError(EncodingError::Code::LimitExceeded, message);
}

inline bool isKnown(FormalKind kind)
{
    const auto value = static_cast<Byte>(kind);
    return value >= static_cast<Byte>(FormalKind::Type) && value <= static_cast<Byte>(FormalKind::String);
}

inline bool isKnown(LegacyCanonicalIRNodeKind kind)
{
    const auto value = static_cast<Byte>(kind);
    return value >= static_cast<Byte>(LegacyCanonicalIRNodeKind::BuiltIn) && value <= static_cast<Byte>(LegacyCanonicalIRNodeKind::ExternalCall);
}

inline bool isKnown(CanonicalIRNodeKind kind)
{
    const auto value = static_cast<Byte>(kind);
    return value >= static_cast<Byte>(CanonicalIRNodeKind::BuiltIn)
        && value <= static_cast<Byte>(CanonicalIRNodeKind::ObjectSkipRegexp);
}

inline bool isKnown(SpecializedEnumWidth width)
{
    return width == SpecializedEnumWidth::Enum8 || width == SpecializedEnumWidth::Enum16;
}

inline bool isKnown(CanonicalFieldKind kind)
{
    const auto value = static_cast<Byte>(kind);
    return value >= static_cast<Byte>(CanonicalFieldKind::Null)
        && value <= static_cast<Byte>(CanonicalFieldKind::AggregateFunctionState);
}

inline bool isKnown(AggregateFunctionNullsAction action)
{
    return action == AggregateFunctionNullsAction::Empty || action == AggregateFunctionNullsAction::RespectNulls
        || action == AggregateFunctionNullsAction::IgnoreNulls;
}

inline bool isComposite(CanonicalFieldKind kind)
{
    return kind == CanonicalFieldKind::Array || kind == CanonicalFieldKind::Tuple || kind == CanonicalFieldKind::Map
        || kind == CanonicalFieldKind::Object;
}

inline void validateCanonicalFieldPayload(CanonicalFieldKind kind, std::span<const Byte> payload, std::string_view aggregate_state_name)
{
    if (!isKnown(kind))
        invalid("canonical template IR V3 Field kind is unknown");

    std::size_t expected_size = 0;
    switch (kind)
    {
        case CanonicalFieldKind::None: invalid("canonical template IR V3 Field kind is absent");
        case CanonicalFieldKind::Null:
        case CanonicalFieldKind::NegativeInfinity:
        case CanonicalFieldKind::PositiveInfinity:
        case CanonicalFieldKind::Array:
        case CanonicalFieldKind::Tuple:
        case CanonicalFieldKind::Map:
        case CanonicalFieldKind::Object: expected_size = 0; break;
        case CanonicalFieldKind::Bool:
            expected_size = 1;
            if (payload.size() == expected_size && payload.front() > 1)
                invalid("canonical template IR V3 Boolean Field payload is noncanonical");
            break;
        case CanonicalFieldKind::IPv4: expected_size = 4; break;
        case CanonicalFieldKind::UInt64:
        case CanonicalFieldKind::Int64:
        case CanonicalFieldKind::Float64:
        case CanonicalFieldKind::Decimal32: expected_size = 8; break;
        case CanonicalFieldKind::Decimal64: expected_size = 12; break;
        case CanonicalFieldKind::UInt128:
        case CanonicalFieldKind::Int128:
        case CanonicalFieldKind::UUID:
        case CanonicalFieldKind::IPv6: expected_size = 16; break;
        case CanonicalFieldKind::Decimal128: expected_size = 20; break;
        case CanonicalFieldKind::UInt256:
        case CanonicalFieldKind::Int256: expected_size = 32; break;
        case CanonicalFieldKind::Decimal256: expected_size = 36; break;
        case CanonicalFieldKind::String:
        case CanonicalFieldKind::AggregateFunctionState: expected_size = payload.size(); break;
    }
    if (payload.size() != expected_size)
        invalid("canonical template IR V3 Field payload has the wrong size");
    if (kind != CanonicalFieldKind::AggregateFunctionState && !aggregate_state_name.empty())
        invalid("canonical template IR V3 aggregate-function-state name is noncanonical");
}

inline std::uint64_t encodeZigZag(std::int64_t value)
{
    if (value >= 0)
        return static_cast<std::uint64_t>(value) << 1;
    const auto magnitude = static_cast<std::uint64_t>(-(value + 1)) + 1;
    return (magnitude << 1) - 1;
}

inline std::int64_t decodeZigZag(std::uint64_t value)
{
    const auto magnitude = (value >> 1) + (value & 1);
    if ((value & 1) == 0)
        return static_cast<std::int64_t>(magnitude);
    if (magnitude == (std::uint64_t{1} << 63))
        return std::numeric_limits<std::int64_t>::min();
    return -static_cast<std::int64_t>(magnitude);
}

inline bool containsZeroByte(std::string_view value)
{
    return std::find(value.begin(), value.end(), '\0') != value.end();
}

inline int compareBinaryStrings(std::string_view lhs, std::string_view rhs)
{
    const auto size = std::min(lhs.size(), rhs.size());
    for (std::size_t index = 0; index < size; ++index)
    {
        const auto left = static_cast<unsigned char>(lhs[index]);
        const auto right = static_cast<unsigned char>(rhs[index]);
        if (left != right)
            return left < right ? -1 : 1;
    }
    if (lhs.size() == rhs.size())
        return 0;
    return lhs.size() < rhs.size() ? -1 : 1;
}

inline bool isUnsigned(FormalKind kind)
{
    return kind == FormalKind::UInt8 || kind == FormalKind::UInt16 || kind == FormalKind::UInt32 || kind == FormalKind::UInt64;
}

template <std::size_t size>
int compareArray(const std::array<Byte, size> & lhs, const std::array<Byte, size> & rhs)
{
    if (std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end()))
        return -1;
    if (std::lexicographical_compare(rhs.begin(), rhs.end(), lhs.begin(), lhs.end()))
        return 1;
    return 0;
}

/// Definition hashes commit the target selected by an immutable identity; they
/// do not create a second identity or participate in canonical set ordering.
inline int compareDependencyStableIdentity(const DependencyIdentity & lhs, const DependencyIdentity & rhs)
{
    if (const int result = compareArray(lhs.database_uuid, rhs.database_uuid))
        return result;
    if (const int result = compareArray(lhs.type_uuid, rhs.type_uuid))
        return result;
    if (lhs.revision != rhs.revision)
        return lhs.revision < rhs.revision ? -1 : 1;
    return 0;
}

inline int compareSemanticDependencyStableIdentity(const SemanticDependency & lhs, const SemanticDependency & rhs)
{
    if (const int result = compareArray(lhs.type_uuid, rhs.type_uuid))
        return result;
    if (lhs.revision != rhs.revision)
        return lhs.revision < rhs.revision ? -1 : 1;
    return 0;
}

class SpanWriter final
{
public:
    SpanWriter(std::span<Byte> output_, std::uint64_t maximum_bytes_)
        : output(output_)
        , maximum_bytes(maximum_bytes_)
    {
    }

    void writeByte(Byte value)
    {
        reserve(1);
        if (!output.empty())
            output[position - 1] = value;
    }

    void writeUInt16LE(std::uint16_t value)
    {
        writeByte(static_cast<Byte>(value));
        writeByte(static_cast<Byte>(value >> 8));
    }

    void writeUInt64LE(std::uint64_t value)
    {
        for (unsigned shift = 0; shift < 64; shift += 8)
            writeByte(static_cast<Byte>(value >> shift));
    }

    void writeVarUInt(std::uint64_t value)
    {
        do
        {
            Byte byte = static_cast<Byte>(value & 0x7f);
            value >>= 7;
            if (value)
                byte |= 0x80;
            writeByte(byte);
        } while (value);
    }

    void writeBytes(std::string_view value)
    {
        reserve(value.size());
        if (!output.empty())
        {
            const auto begin = position - value.size();
            std::transform(value.begin(), value.end(), output.begin() + begin, [](char byte) { return static_cast<Byte>(byte); });
        }
    }

    template <std::size_t size>
    void writeBytes(const std::array<Byte, size> & value)
    {
        reserve(size);
        if (!output.empty())
            std::copy(value.begin(), value.end(), output.begin() + position - size);
    }

    void writeBytes(std::span<const Byte> value)
    {
        reserve(value.size());
        if (!output.empty())
            std::copy(value.begin(), value.end(), output.begin() + position - value.size());
    }

    void writeFrame(std::string_view value)
    {
        if (!std::in_range<std::uint64_t>(value.size()))
            limit("frame length does not fit the canonical length domain");
        writeVarUInt(static_cast<std::uint64_t>(value.size()));
        writeBytes(value);
    }

    void writeFrame(std::span<const Byte> value)
    {
        if (!std::in_range<std::uint64_t>(value.size()))
            limit("frame length does not fit the canonical length domain");
        writeVarUInt(static_cast<std::uint64_t>(value.size()));
        writeBytes(value);
    }

    std::size_t size() const { return position; }

private:
    void reserve(std::size_t addition)
    {
        if (!std::in_range<std::uint64_t>(addition))
            limit("encoded byte count does not fit the canonical length domain");
        const auto addition_u64 = static_cast<std::uint64_t>(addition);
        if (addition_u64 > maximum_bytes || position > maximum_bytes - addition_u64)
            limit("canonical checker encoding exceeds its byte limit");
        if (addition > std::numeric_limits<std::size_t>::max() - position)
            limit("canonical checker encoding overflows size_t");
        const std::size_t prospective = position + addition;
        if (!output.empty() && prospective > output.size())
            throw EncodingError(EncodingError::Code::OutputSizeMismatch, "canonical checker output span is too small");
        position = prospective;
    }

    std::span<Byte> output;
    std::uint64_t maximum_bytes;
    std::size_t position = 0;
};

inline void requireExactOutput(const SpanWriter & writer, std::span<Byte> output)
{
    if (!output.empty() && writer.size() != output.size())
        throw EncodingError(EncodingError::Code::OutputSizeMismatch, "canonical checker output span is not exact");
}

inline void writeDomain(SpanWriter & writer, std::string_view domain)
{
    writer.writeBytes(domain);
    writer.writeByte(0);
}

inline void validateDependencies(std::span<const DependencyIdentity> dependencies, std::uint64_t maximum_dependencies)
{
    if (dependencies.size() > maximum_dependencies)
        limit("dependency count exceeds its limit");
    for (std::size_t index = 1; index < dependencies.size(); ++index)
    {
        if (compareDependencyStableIdentity(dependencies[index - 1], dependencies[index]) >= 0)
            invalid("dependencies are not in strictly increasing canonical order");
    }
}

inline void validateSemanticDependencies(std::span<const SemanticDependency> dependencies, const EncodingLimits & limits)
{
    if (dependencies.size() > limits.maximum_direct_dependencies)
        limit("semantic dependency count exceeds its limit");
    for (std::size_t index = 1; index < dependencies.size(); ++index)
    {
        if (compareSemanticDependencyStableIdentity(dependencies[index - 1], dependencies[index]) >= 0)
            invalid("semantic dependencies are not in strictly increasing canonical order");
    }
}

inline void validateCompositionalDependencies(std::span<const CompositionalDependency> dependencies, const EncodingLimits & limits)
{
    if (dependencies.size() > limits.maximum_direct_dependencies)
        limit("compositional dependency count exceeds its limit");
    for (std::size_t index = 1; index < dependencies.size(); ++index)
    {
        if (compareDependencyStableIdentity(dependencies[index - 1].identity, dependencies[index].identity) >= 0)
            invalid("compositional dependencies are not in strictly increasing canonical order");
    }
}

inline void writeDependencies(SpanWriter & writer, std::span<const DependencyIdentity> dependencies)
{
    writer.writeVarUInt(static_cast<std::uint64_t>(dependencies.size()));
    for (const auto & dependency : dependencies)
    {
        writer.writeBytes(dependency.database_uuid);
        writer.writeBytes(dependency.type_uuid);
        writer.writeUInt64LE(dependency.revision);
        writer.writeBytes(dependency.target_definition_hash);
    }
}

inline void writeSemanticDependencies(SpanWriter & writer, std::span<const SemanticDependency> dependencies)
{
    writer.writeVarUInt(static_cast<std::uint64_t>(dependencies.size()));
    for (const auto & dependency : dependencies)
    {
        writer.writeBytes(dependency.type_uuid);
        writer.writeUInt64LE(dependency.revision);
        writer.writeBytes(dependency.target_definition_hash);
    }
}

inline void writeCompositionalDependencies(SpanWriter & writer, std::span<const CompositionalDependency> dependencies)
{
    writer.writeVarUInt(static_cast<std::uint64_t>(dependencies.size()));
    for (const auto & dependency : dependencies)
    {
        writer.writeBytes(dependency.identity.database_uuid);
        writer.writeBytes(dependency.identity.type_uuid);
        writer.writeUInt64LE(dependency.identity.revision);
        writer.writeBytes(dependency.identity.target_definition_hash);
        writer.writeBytes(dependency.target_compositional_closure_digest);
    }
}

inline void validateLegacyCanonicalTemplateIR(const LegacyCanonicalTemplateIRView & canonical_ir, const EncodingLimits & limits)
{
    if (canonical_ir.formal_count > limits.maximum_formals)
        limit("canonical template IR formal count exceeds its limit");
    if (canonical_ir.direct_dependency_count > limits.maximum_direct_dependencies)
        limit("canonical template IR direct-dependency count exceeds its limit");
    if (canonical_ir.nodes.empty())
        invalid("canonical template IR has no root node");
    if (canonical_ir.nodes.size() > limits.maximum_logical_nodes)
        limit("canonical template IR node count exceeds its limit");

    std::uint64_t edge_count = 0;
    std::uint64_t atom_bytes = 0;
    std::size_t next_discovered_node = 1;
    const auto chargeAtomBytes = [&](std::string_view value)
    {
        if (!std::in_range<std::uint64_t>(value.size()))
            limit("canonical template IR atom bytes do not fit the canonical length domain");
        const auto size = static_cast<std::uint64_t>(value.size());
        if (size > limits.maximum_ir_atom_bytes || atom_bytes > limits.maximum_ir_atom_bytes - size)
            limit("canonical template IR atom bytes exceed their limit");
        atom_bytes += size;
    };
    const auto chargeEdges = [&](std::size_t count)
    {
        if (!std::in_range<std::uint64_t>(count))
            limit("canonical template IR edge count does not fit the canonical length domain");
        const auto count_u64 = static_cast<std::uint64_t>(count);
        if (count_u64 > limits.maximum_template_edges || edge_count > limits.maximum_template_edges - count_u64)
            limit("canonical template IR edge count exceeds its limit");
        edge_count += count_u64;
    };
    const auto validateTemplateChild = [&](std::size_t source, const CanonicalIRChildView & child)
    {
        if (child.reference <= source || child.reference >= canonical_ir.nodes.size())
            invalid("canonical template IR child is not in strict parent-before-child order");
        if (child.reference >= next_discovered_node)
        {
            if (child.reference != next_discovered_node)
                invalid("canonical template IR node IDs are not in first-discovery order");
            ++next_discovered_node;
        }
    };

    for (std::size_t source = 0; source < canonical_ir.nodes.size(); ++source)
    {
        if (source >= next_discovered_node)
            invalid("canonical template IR contains an unreachable node");

        const auto & node = canonical_ir.nodes[source];
        if (!isKnown(node.kind))
            invalid("canonical template IR node kind is unknown");
        switch (node.kind)
        {
            case LegacyCanonicalIRNodeKind::BuiltIn:
                if (node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0 || node.boolean_literal
                    || node.dependency_ordinal != 0)
                    invalid("canonical template IR built-in node is noncanonical");
                chargeAtomBytes(node.atom);
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    chargeAtomBytes(child.label);
                    validateTemplateChild(source, child);
                }
                break;
            case LegacyCanonicalIRNodeKind::TypeFormal:
            case LegacyCanonicalIRNodeKind::ValueFormal:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 0 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("canonical template IR formal node is noncanonical");
                break;
            case LegacyCanonicalIRNodeKind::UnsignedLiteral:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || !node.children.empty())
                    invalid("canonical template IR unsigned-literal node is noncanonical");
                break;
            case LegacyCanonicalIRNodeKind::BooleanLiteral:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("canonical template IR Boolean-literal node is noncanonical");
                break;
            case LegacyCanonicalIRNodeKind::TypeIfZero:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 0 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || node.children.size() != 2)
                    invalid("canonical template IR TYPE_IF node is noncanonical");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (!child.label.empty())
                        invalid("canonical template IR TYPE_IF edge has a label");
                    validateTemplateChild(source, child);
                }
                break;
            case LegacyCanonicalIRNodeKind::SelfCall:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 1 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty())
                    invalid("canonical template IR self-call node is noncanonical");
                break;
            case LegacyCanonicalIRNodeKind::ExternalCall:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0 || node.boolean_literal
                    || node.dependency_ordinal >= canonical_ir.direct_dependency_count)
                    invalid("canonical template IR external-call node is noncanonical");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (!child.label.empty() || child.reference >= canonical_ir.formal_count)
                        invalid("canonical template IR external-call actual is not a caller-formal ordinal");
                }
                break;
        }
    }

    if (next_discovered_node != canonical_ir.nodes.size())
        invalid("canonical template IR contains an unreachable node");
}

inline void writeLegacyCanonicalTemplateIR(SpanWriter & writer, const LegacyCanonicalTemplateIRView & canonical_ir)
{
    writer.writeUInt16LE(legacy_canonical_ir_version);
    writer.writeUInt16LE(canonical_ir.formal_count);
    writer.writeUInt16LE(canonical_ir.direct_dependency_count);
    writer.writeVarUInt(static_cast<std::uint64_t>(canonical_ir.nodes.size()));
    for (const auto & node : canonical_ir.nodes)
    {
        writer.writeByte(static_cast<Byte>(node.kind));
        switch (node.kind)
        {
            case LegacyCanonicalIRNodeKind::BuiltIn:
                writer.writeFrame(node.atom);
                writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                for (const auto & child : node.children)
                {
                    writer.writeFrame(child.label);
                    writer.writeVarUInt(child.reference);
                }
                break;
            case LegacyCanonicalIRNodeKind::TypeFormal:
            case LegacyCanonicalIRNodeKind::ValueFormal: writer.writeUInt16LE(node.parameter); break;
            case LegacyCanonicalIRNodeKind::UnsignedLiteral: writer.writeUInt64LE(node.unsigned_literal); break;
            case LegacyCanonicalIRNodeKind::BooleanLiteral: writer.writeByte(node.boolean_literal ? 1 : 0); break;
            case LegacyCanonicalIRNodeKind::TypeIfZero:
                writer.writeUInt16LE(node.parameter);
                writer.writeVarUInt(node.children[0].reference);
                writer.writeVarUInt(node.children[1].reference);
                break;
            case LegacyCanonicalIRNodeKind::SelfCall:
                writer.writeUInt16LE(node.parameter);
                writer.writeVarUInt(node.decrement);
                break;
            case LegacyCanonicalIRNodeKind::ExternalCall:
                writer.writeUInt16LE(node.dependency_ordinal);
                writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                for (const auto & child : node.children)
                    writer.writeUInt16LE(static_cast<std::uint16_t>(child.reference));
                break;
        }
    }
}

/// Canonical-template-IR V3 wire contract:
///
///   domain || 0 || u16le(3) || u16le(formals) || u16le(dependencies)
///          || minimal_varuint(node_count) || nodes...
///
/// Node tags 1..8 retain the V2 meanings. V3 encodes unsigned literals as a
/// minimal VarUInt, signed literals as a minimal ZigZag VarUInt, strings and
/// identifiers as byte-length-framed payloads, and a specialized Enum as
/// width-byte || entry-count || repeated(name-frame || ZigZag value). Enum
/// entries are inline: they do not consume node IDs and cannot be referenced.
/// No generic SQL/expression payload exists. All fixed-width integers are
/// little-endian. Every template reference is forward and node IDs are dense
/// in first-discovery order.
inline void validateCanonicalTemplateIR(const CanonicalTemplateIRView & canonical_ir, const EncodingLimits & limits)
{
    if (canonical_ir.formal_count > limits.maximum_formals)
        limit("canonical template IR V3 formal count exceeds its limit");
    if (canonical_ir.direct_dependency_count > limits.maximum_direct_dependencies)
        limit("canonical template IR V3 direct-dependency count exceeds its limit");
    if (canonical_ir.nodes.empty())
        invalid("canonical template IR V3 has no root node");
    if (canonical_ir.nodes.size() > limits.maximum_logical_nodes)
        limit("canonical template IR V3 node count exceeds its limit");
    if (canonical_ir.nodes.size() > static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1)
        limit("canonical template IR V3 node count exceeds its reference domain");

    std::uint64_t edge_count = 0;
    std::uint64_t atom_bytes = 0;
    std::uint64_t literal_bytes = 0;
    std::uint64_t enum_entry_count = 0;
    std::size_t next_discovered_node = 1;
    const auto chargeBytes = [](std::string_view value, std::uint64_t & total, std::uint64_t maximum, std::string_view message)
    {
        if (!std::in_range<std::uint64_t>(value.size()))
            limit("canonical template IR V3 byte count does not fit the canonical length domain");
        const auto size = static_cast<std::uint64_t>(value.size());
        if (size > maximum || total > maximum - size)
            limit(message);
        total += size;
    };
    const auto chargeLiteralBytes = [&](std::size_t size)
    {
        if (!std::in_range<std::uint64_t>(size))
            limit("canonical template IR V3 byte count does not fit the canonical length domain");
        const auto size_u64 = static_cast<std::uint64_t>(size);
        if (size_u64 > limits.maximum_ir_literal_bytes || literal_bytes > limits.maximum_ir_literal_bytes - size_u64)
            limit("canonical template IR V3 literal bytes exceed their limit");
        literal_bytes += size_u64;
    };
    const auto chargeEdges = [&](std::size_t count)
    {
        if (!std::in_range<std::uint64_t>(count))
            limit("canonical template IR V3 edge count does not fit the canonical length domain");
        const auto count_u64 = static_cast<std::uint64_t>(count);
        if (count_u64 > limits.maximum_template_edges || edge_count > limits.maximum_template_edges - count_u64)
            limit("canonical template IR V3 edge count exceeds its limit");
        edge_count += count_u64;
    };
    const auto validateTemplateChild = [&](std::size_t source, const CanonicalIRChildView & child)
    {
        if (child.reference <= source || child.reference >= canonical_ir.nodes.size())
            invalid("canonical template IR V3 child is not in strict parent-before-child order");
        if (child.reference >= next_discovered_node)
        {
            if (child.reference != next_discovered_node)
                invalid("canonical template IR V3 node IDs are not in first-discovery order");
            ++next_discovered_node;
        }
    };
    const auto requireNoFieldState = [](const CanonicalIRNodeView & node)
    { return node.field_kind == CanonicalFieldKind::None && node.field_payload.empty() && node.field_name.empty(); };
    const auto requireNoExtendedState = [&](const CanonicalIRNodeView & node)
    {
        return node.text.empty() && node.signed_literal == 0 && node.specialized_enum_width == SpecializedEnumWidth::None
            && node.enum_entries.empty() && requireNoFieldState(node)
            && node.aggregate_nulls_action == AggregateFunctionNullsAction::Empty;
    };

    for (std::size_t source = 0; source < canonical_ir.nodes.size(); ++source)
    {
        if (source >= next_discovered_node)
            invalid("canonical template IR V3 contains an unreachable node");

        const auto & node = canonical_ir.nodes[source];
        if (!isKnown(node.kind))
            invalid("canonical template IR V3 node kind is unknown");
        switch (node.kind)
        {
            case CanonicalIRNodeKind::BuiltIn:
                if (node.atom.empty() || containsZeroByte(node.atom) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 built-in node is noncanonical");
                chargeBytes(node.atom, atom_bytes, limits.maximum_ir_atom_bytes, "canonical template IR V3 atom bytes exceed their limit");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (containsZeroByte(child.label))
                        invalid("canonical template IR V3 built-in edge label contains NUL");
                    chargeBytes(
                        child.label, atom_bytes, limits.maximum_ir_atom_bytes, "canonical template IR V3 atom bytes exceed their limit");
                    validateTemplateChild(source, child);
                }
                break;
            case CanonicalIRNodeKind::TypeFormal:
            case CanonicalIRNodeKind::ValueFormal:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 0 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty() || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 formal node is noncanonical");
                break;
            case CanonicalIRNodeKind::UnsignedLiteral:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || !node.children.empty() || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 unsigned-literal node is noncanonical");
                break;
            case CanonicalIRNodeKind::BooleanLiteral:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.dependency_ordinal != 0 || !node.children.empty() || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 Boolean-literal node is noncanonical");
                break;
            case CanonicalIRNodeKind::TypeIfZero:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 0 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || node.children.size() != 2
                    || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 TYPE_IF node is noncanonical");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (!child.label.empty())
                        invalid("canonical template IR V3 TYPE_IF edge has a label");
                    validateTemplateChild(source, child);
                }
                break;
            case CanonicalIRNodeKind::SelfCall:
                if (!node.atom.empty() || node.parameter >= canonical_ir.formal_count || node.decrement != 1 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty() || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 self-call node is noncanonical");
                break;
            case CanonicalIRNodeKind::ExternalCall:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0 || node.boolean_literal
                    || node.dependency_ordinal >= canonical_ir.direct_dependency_count || !requireNoExtendedState(node))
                    invalid("canonical template IR V3 external-call node is noncanonical");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (!child.label.empty() || child.reference >= canonical_ir.formal_count)
                        invalid("canonical template IR V3 external-call actual is not a caller-formal ordinal");
                }
                break;
            case CanonicalIRNodeKind::SignedLiteral:
                if (!node.atom.empty() || !node.text.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 signed-literal node is noncanonical");
                break;
            case CanonicalIRNodeKind::StringLiteral:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 string-literal node is noncanonical");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                break;
            case CanonicalIRNodeKind::Identifier:
                if (!node.atom.empty() || node.text.empty() || containsZeroByte(node.text) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || !node.children.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 identifier node is noncanonical");
                if (node.text.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 identifier exceeds its byte limit");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                break;
            case CanonicalIRNodeKind::SpecializedEnum: {
                if (!node.atom.empty() || !node.text.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty()
                    || !isKnown(node.specialized_enum_width) || node.enum_entries.empty() || !requireNoFieldState(node)
                    || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 specialized-Enum node is noncanonical");
                if (!std::in_range<std::uint64_t>(node.enum_entries.size()))
                    limit("canonical template IR V3 Enum entry count does not fit the canonical length domain");
                const auto count = static_cast<std::uint64_t>(node.enum_entries.size());
                if (count > limits.maximum_ir_enum_entries || enum_entry_count > limits.maximum_ir_enum_entries - count)
                    limit("canonical template IR V3 Enum entry count exceeds its limit");
                enum_entry_count += count;
                bool has_previous = false;
                std::int64_t previous = 0;
                for (const auto & entry : node.enum_entries)
                {
                    const bool in_range = node.specialized_enum_width == SpecializedEnumWidth::Enum8
                        ? entry.value >= std::numeric_limits<std::int8_t>::min() && entry.value <= std::numeric_limits<std::int8_t>::max()
                        : entry.value >= std::numeric_limits<std::int16_t>::min()
                            && entry.value <= std::numeric_limits<std::int16_t>::max();
                    if (!in_range)
                        invalid("canonical template IR V3 Enum value is out of range");
                    if (has_previous && entry.value <= previous)
                        invalid("canonical template IR V3 Enum entries are not in canonical numeric order");
                    has_previous = true;
                    previous = entry.value;
                    chargeBytes(
                        entry.name,
                        literal_bytes,
                        limits.maximum_ir_literal_bytes,
                        "canonical template IR V3 literal bytes exceed their limit");
                }
                break;
            }
            case CanonicalIRNodeKind::FieldValue:
                if (!node.atom.empty() || !node.text.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 Field-value node is noncanonical");
                validateCanonicalFieldPayload(node.field_kind, node.field_payload, node.field_name);
                chargeLiteralBytes(node.field_payload.size());
                chargeLiteralBytes(node.field_name.size());
                if (isComposite(node.field_kind))
                {
                    if (node.field_kind == CanonicalFieldKind::Map && (node.children.size() % 2) != 0)
                        invalid("canonical template IR V3 Map Field has odd key/value arity");
                    chargeEdges(node.children.size());
                    std::string_view previous_object_key;
                    bool has_previous_object_key = false;
                    for (const auto & child : node.children)
                    {
                        if (node.field_kind == CanonicalFieldKind::Object)
                        {
                            chargeLiteralBytes(child.label.size());
                            if (has_previous_object_key && compareBinaryStrings(previous_object_key, child.label) >= 0)
                                invalid("canonical template IR V3 Object Field keys are not in strict binary order");
                            previous_object_key = child.label;
                            has_previous_object_key = true;
                        }
                        else if (!child.label.empty())
                        {
                            invalid("canonical template IR V3 non-Object Field edge has a label");
                        }
                        validateTemplateChild(source, child);
                        if (canonical_ir.nodes[child.reference].kind != CanonicalIRNodeKind::FieldValue)
                            invalid("canonical template IR V3 composite Field child is not a Field value");
                    }
                }
                else if (!node.children.empty())
                {
                    invalid("canonical template IR V3 scalar/opaque Field has structural children");
                }
                break;
            case CanonicalIRNodeKind::AggregateFunction:
                if (!node.atom.empty() || node.text.empty() || containsZeroByte(node.text) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || !isKnown(node.aggregate_nulls_action))
                    invalid("canonical template IR V3 aggregate-function node is noncanonical");
                if (node.text.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 aggregate-function name exceeds its byte limit");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                chargeEdges(node.children.size());
                for (const auto & child : node.children)
                {
                    if (!child.label.empty())
                        invalid("canonical template IR V3 aggregate-function parameter has a label");
                    validateTemplateChild(source, child);
                    const auto child_kind = canonical_ir.nodes[child.reference].kind;
                    if (child_kind != CanonicalIRNodeKind::FieldValue && child_kind != CanonicalIRNodeKind::ValueFormal)
                        invalid("canonical template IR V3 aggregate-function parameter is not a Field value or value formal");
                }
                break;
            case CanonicalIRNodeKind::DynamicSetting:
            case CanonicalIRNodeKind::ObjectSetting:
                if (!node.atom.empty() || node.text.empty() || containsZeroByte(node.text) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || node.children.size() != 1 || !node.children.front().label.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 named-setting node is noncanonical");
                if ((node.kind == CanonicalIRNodeKind::DynamicSetting && node.text != "max_types")
                    || (node.kind == CanonicalIRNodeKind::ObjectSetting && node.text != "max_dynamic_types"
                        && node.text != "max_dynamic_paths"))
                    invalid("canonical template IR V3 named setting is unknown");
                if (node.text.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 setting name exceeds its byte limit");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                chargeEdges(1);
                validateTemplateChild(source, node.children.front());
                break;
            case CanonicalIRNodeKind::ObjectTypedPath:
                if (!node.atom.empty() || node.text.empty() || containsZeroByte(node.text) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || node.children.size() != 1 || !node.children.front().label.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 typed Object path is noncanonical");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                chargeEdges(1);
                validateTemplateChild(source, node.children.front());
                break;
            case CanonicalIRNodeKind::ObjectSkipPath:
                if (!node.atom.empty() || node.text.empty() || containsZeroByte(node.text) || node.parameter != 0 || node.decrement != 0
                    || node.unsigned_literal != 0 || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0
                    || !node.children.empty() || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 skipped Object path is noncanonical");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                break;
            case CanonicalIRNodeKind::ObjectSkipRegexp:
                if (!node.atom.empty() || node.parameter != 0 || node.decrement != 0 || node.unsigned_literal != 0
                    || node.signed_literal != 0 || node.boolean_literal || node.dependency_ordinal != 0 || !node.children.empty()
                    || node.specialized_enum_width != SpecializedEnumWidth::None || !node.enum_entries.empty()
                    || !requireNoFieldState(node) || node.aggregate_nulls_action != AggregateFunctionNullsAction::Empty)
                    invalid("canonical template IR V3 skipped Object regexp is noncanonical");
                chargeBytes(
                    node.text, literal_bytes, limits.maximum_ir_literal_bytes, "canonical template IR V3 literal bytes exceed their limit");
                break;
        }
    }

    if (next_discovered_node != canonical_ir.nodes.size())
        invalid("canonical template IR V3 contains an unreachable node");
}

inline void writeCanonicalTemplateIR(SpanWriter & writer, const CanonicalTemplateIRView & canonical_ir)
{
    writeDomain(writer, canonical_template_ir_domain);
    writer.writeUInt16LE(canonical_ir_version);
    writer.writeUInt16LE(canonical_ir.formal_count);
    writer.writeUInt16LE(canonical_ir.direct_dependency_count);
    writer.writeVarUInt(static_cast<std::uint64_t>(canonical_ir.nodes.size()));
    for (const auto & node : canonical_ir.nodes)
    {
        writer.writeByte(static_cast<Byte>(node.kind));
        switch (node.kind)
        {
            case CanonicalIRNodeKind::BuiltIn:
                writer.writeFrame(node.atom);
                writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                for (const auto & child : node.children)
                {
                    writer.writeFrame(child.label);
                    writer.writeVarUInt(child.reference);
                }
                break;
            case CanonicalIRNodeKind::TypeFormal:
            case CanonicalIRNodeKind::ValueFormal: writer.writeUInt16LE(node.parameter); break;
            case CanonicalIRNodeKind::UnsignedLiteral: writer.writeVarUInt(node.unsigned_literal); break;
            case CanonicalIRNodeKind::BooleanLiteral: writer.writeByte(node.boolean_literal ? 1 : 0); break;
            case CanonicalIRNodeKind::TypeIfZero:
                writer.writeUInt16LE(node.parameter);
                writer.writeVarUInt(node.children[0].reference);
                writer.writeVarUInt(node.children[1].reference);
                break;
            case CanonicalIRNodeKind::SelfCall:
                writer.writeUInt16LE(node.parameter);
                writer.writeVarUInt(node.decrement);
                break;
            case CanonicalIRNodeKind::ExternalCall:
                writer.writeUInt16LE(node.dependency_ordinal);
                writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                for (const auto & child : node.children)
                    writer.writeUInt16LE(static_cast<std::uint16_t>(child.reference));
                break;
            case CanonicalIRNodeKind::SignedLiteral: writer.writeVarUInt(encodeZigZag(node.signed_literal)); break;
            case CanonicalIRNodeKind::StringLiteral:
            case CanonicalIRNodeKind::Identifier: writer.writeFrame(node.text); break;
            case CanonicalIRNodeKind::SpecializedEnum:
                writer.writeByte(static_cast<Byte>(node.specialized_enum_width));
                writer.writeVarUInt(static_cast<std::uint64_t>(node.enum_entries.size()));
                for (const auto & entry : node.enum_entries)
                {
                    writer.writeFrame(entry.name);
                    writer.writeVarUInt(encodeZigZag(entry.value));
                }
                break;
            case CanonicalIRNodeKind::FieldValue:
                writer.writeByte(static_cast<Byte>(node.field_kind));
                switch (node.field_kind)
                {
                    case CanonicalFieldKind::None: invalid("canonical template IR V3 Field kind is absent");
                    case CanonicalFieldKind::Null:
                    case CanonicalFieldKind::NegativeInfinity:
                    case CanonicalFieldKind::PositiveInfinity: break;
                    case CanonicalFieldKind::String: writer.writeFrame(node.field_payload); break;
                    case CanonicalFieldKind::AggregateFunctionState:
                        writer.writeFrame(node.field_name);
                        writer.writeFrame(node.field_payload);
                        break;
                    case CanonicalFieldKind::Array:
                    case CanonicalFieldKind::Tuple:
                    case CanonicalFieldKind::Map:
                    case CanonicalFieldKind::Object:
                        writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                        for (const auto & child : node.children)
                        {
                            if (node.field_kind == CanonicalFieldKind::Object)
                                writer.writeFrame(child.label);
                            writer.writeVarUInt(child.reference);
                        }
                        break;
                    case CanonicalFieldKind::UInt64:
                    case CanonicalFieldKind::Int64:
                    case CanonicalFieldKind::Float64:
                    case CanonicalFieldKind::Bool:
                    case CanonicalFieldKind::UInt128:
                    case CanonicalFieldKind::Int128:
                    case CanonicalFieldKind::UInt256:
                    case CanonicalFieldKind::Int256:
                    case CanonicalFieldKind::Decimal32:
                    case CanonicalFieldKind::Decimal64:
                    case CanonicalFieldKind::Decimal128:
                    case CanonicalFieldKind::Decimal256:
                    case CanonicalFieldKind::UUID:
                    case CanonicalFieldKind::IPv4:
                    case CanonicalFieldKind::IPv6: writer.writeBytes(node.field_payload); break;
                }
                break;
            case CanonicalIRNodeKind::AggregateFunction:
                writer.writeFrame(node.text);
                writer.writeByte(static_cast<Byte>(node.aggregate_nulls_action));
                writer.writeVarUInt(static_cast<std::uint64_t>(node.children.size()));
                for (const auto & child : node.children)
                    writer.writeVarUInt(child.reference);
                break;
            case CanonicalIRNodeKind::DynamicSetting:
            case CanonicalIRNodeKind::ObjectSetting:
            case CanonicalIRNodeKind::ObjectTypedPath:
                writer.writeFrame(node.text);
                writer.writeVarUInt(node.children.front().reference);
                break;
            case CanonicalIRNodeKind::ObjectSkipPath:
            case CanonicalIRNodeKind::ObjectSkipRegexp: writer.writeFrame(node.text); break;
        }
    }
}

class CanonicalIRReader final
{
public:
    explicit CanonicalIRReader(std::span<const Byte> input_)
        : input(input_)
    {
    }

    Byte readByte()
    {
        if (position == input.size())
            invalid("canonical template IR V3 encoding is truncated");
        return input[position++];
    }

    std::uint16_t readUInt16LE()
    {
        const auto low = static_cast<std::uint16_t>(readByte());
        return static_cast<std::uint16_t>(low | (static_cast<std::uint16_t>(readByte()) << 8));
    }

    std::uint64_t readVarUInt()
    {
        std::uint64_t result = 0;
        for (unsigned index = 0; index < 10; ++index)
        {
            const Byte byte = readByte();
            const Byte payload = byte & 0x7f;
            if (index == 9 && payload > 1)
                invalid("canonical template IR V3 VarUInt overflows UInt64");
            result |= static_cast<std::uint64_t>(payload) << (index * 7);
            if ((byte & 0x80) == 0)
            {
                if (index != 0 && payload == 0)
                    invalid("canonical template IR V3 VarUInt is not minimally encoded");
                return result;
            }
        }
        invalid("canonical template IR V3 VarUInt is unterminated");
    }

    std::span<const Byte> readFrame()
    {
        const auto length = readVarUInt();
        if (length > remaining())
            invalid("canonical template IR V3 frame is truncated");
        if (!std::in_range<std::size_t>(length))
            limit("canonical template IR V3 frame length does not fit size_t");
        const auto size = static_cast<std::size_t>(length);
        const auto result = input.subspan(position, size);
        position += size;
        return result;
    }

    std::span<const Byte> readBytes(std::size_t size)
    {
        if (size > remaining())
            invalid("canonical template IR V3 fixed-width payload is truncated");
        const auto result = input.subspan(position, size);
        position += size;
        return result;
    }

    void readDomain(std::string_view domain)
    {
        for (const char expected : domain)
        {
            if (readByte() != static_cast<Byte>(expected))
                invalid("canonical template IR V3 domain is invalid");
        }
        if (readByte() != 0)
            invalid("canonical template IR V3 domain is not terminated");
    }

    void requireEnd() const
    {
        if (position != input.size())
            invalid("canonical template IR V3 encoding has trailing bytes");
    }

private:
    std::size_t remaining() const { return input.size() - position; }

    std::span<const Byte> input;
    std::size_t position = 0;
};

inline bool containsZeroByte(std::span<const Byte> value)
{
    return std::find(value.begin(), value.end(), Byte{0}) != value.end();
}

inline int compareBinarySpans(std::span<const Byte> lhs, std::span<const Byte> rhs)
{
    if (std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end()))
        return -1;
    if (std::lexicographical_compare(rhs.begin(), rhs.end(), lhs.begin(), lhs.end()))
        return 1;
    return 0;
}

inline void validateEncodedCanonicalTemplateIR(std::span<const Byte> input, const EncodingLimits & limits)
{
    const auto maximum_bytes = std::min(limits.maximum_output_bytes, limits.maximum_template_ir_bytes);
    if (input.size() > maximum_bytes)
        limit("canonical template IR V3 encoding exceeds its byte limit");

    CanonicalIRReader reader(input);
    reader.readDomain(canonical_template_ir_domain);
    if (reader.readUInt16LE() != canonical_ir_version)
        invalid("canonical template IR V3 encoding has an unknown version");
    const auto formal_count = reader.readUInt16LE();
    const auto direct_dependency_count = reader.readUInt16LE();
    if (formal_count > limits.maximum_formals)
        limit("canonical template IR V3 formal count exceeds its limit");
    if (direct_dependency_count > limits.maximum_direct_dependencies)
        limit("canonical template IR V3 direct-dependency count exceeds its limit");

    const auto node_count = reader.readVarUInt();
    if (node_count == 0)
        invalid("canonical template IR V3 has no root node");
    if (node_count > limits.maximum_logical_nodes)
        limit("canonical template IR V3 node count exceeds its limit");
    if (node_count > static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 1)
        limit("canonical template IR V3 node count exceeds its reference domain");

    std::uint64_t edge_count = 0;
    std::uint64_t atom_bytes = 0;
    std::uint64_t literal_bytes = 0;
    std::uint64_t enum_entry_count = 0;
    std::uint64_t next_discovered_node = 1;
    const auto chargeBytes = [](std::size_t size, std::uint64_t & total, std::uint64_t maximum, std::string_view message)
    {
        if (!std::in_range<std::uint64_t>(size))
            limit("canonical template IR V3 byte count does not fit the canonical length domain");
        const auto size_u64 = static_cast<std::uint64_t>(size);
        if (size_u64 > maximum || total > maximum - size_u64)
            limit(message);
        total += size_u64;
    };
    const auto chargeEdges = [&](std::uint64_t count)
    {
        if (count > limits.maximum_template_edges || edge_count > limits.maximum_template_edges - count)
            limit("canonical template IR V3 edge count exceeds its limit");
        edge_count += count;
    };
    const auto validateTemplateReference = [&](std::uint64_t source, std::uint64_t reference)
    {
        if (reference > std::numeric_limits<std::uint32_t>::max() || reference <= source || reference >= node_count)
            invalid("canonical template IR V3 child is not in strict parent-before-child order");
        if (reference >= next_discovered_node)
        {
            if (reference != next_discovered_node)
                invalid("canonical template IR V3 node IDs are not in first-discovery order");
            ++next_discovered_node;
        }
    };

    for (std::uint64_t source = 0; source < node_count; ++source)
    {
        if (source >= next_discovered_node)
            invalid("canonical template IR V3 contains an unreachable node");
        const auto kind = static_cast<CanonicalIRNodeKind>(reader.readByte());
        if (!isKnown(kind))
            invalid("canonical template IR V3 node kind is unknown");
        switch (kind)
        {
            case CanonicalIRNodeKind::BuiltIn: {
                const auto atom = reader.readFrame();
                if (atom.empty() || containsZeroByte(atom))
                    invalid("canonical template IR V3 built-in atom is empty or contains NUL");
                chargeBytes(
                    atom.size(), atom_bytes, limits.maximum_ir_atom_bytes, "canonical template IR V3 atom bytes exceed their limit");
                const auto child_count = reader.readVarUInt();
                chargeEdges(child_count);
                for (std::uint64_t index = 0; index < child_count; ++index)
                {
                    const auto label = reader.readFrame();
                    if (containsZeroByte(label))
                        invalid("canonical template IR V3 built-in edge label contains NUL");
                    chargeBytes(
                        label.size(), atom_bytes, limits.maximum_ir_atom_bytes, "canonical template IR V3 atom bytes exceed their limit");
                    validateTemplateReference(source, reader.readVarUInt());
                }
                break;
            }
            case CanonicalIRNodeKind::TypeFormal:
            case CanonicalIRNodeKind::ValueFormal:
                if (reader.readUInt16LE() >= formal_count)
                    invalid("canonical template IR V3 formal ordinal is out of range");
                break;
            case CanonicalIRNodeKind::UnsignedLiteral: static_cast<void>(reader.readVarUInt()); break;
            case CanonicalIRNodeKind::BooleanLiteral:
                if (reader.readByte() > 1)
                    invalid("canonical template IR V3 Boolean literal is noncanonical");
                break;
            case CanonicalIRNodeKind::TypeIfZero: {
                if (reader.readUInt16LE() >= formal_count)
                    invalid("canonical template IR V3 TYPE_IF formal ordinal is out of range");
                chargeEdges(2);
                validateTemplateReference(source, reader.readVarUInt());
                validateTemplateReference(source, reader.readVarUInt());
                break;
            }
            case CanonicalIRNodeKind::SelfCall:
                if (reader.readUInt16LE() >= formal_count || reader.readVarUInt() != 1)
                    invalid("canonical template IR V3 self-call node is noncanonical");
                break;
            case CanonicalIRNodeKind::ExternalCall: {
                if (reader.readUInt16LE() >= direct_dependency_count)
                    invalid("canonical template IR V3 dependency ordinal is out of range");
                const auto actual_count = reader.readVarUInt();
                chargeEdges(actual_count);
                for (std::uint64_t index = 0; index < actual_count; ++index)
                {
                    if (reader.readUInt16LE() >= formal_count)
                        invalid("canonical template IR V3 external-call actual is not a caller-formal ordinal");
                }
                break;
            }
            case CanonicalIRNodeKind::SignedLiteral: static_cast<void>(reader.readVarUInt()); break;
            case CanonicalIRNodeKind::StringLiteral: {
                const auto text = reader.readFrame();
                chargeBytes(
                    text.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                break;
            }
            case CanonicalIRNodeKind::Identifier: {
                const auto identifier = reader.readFrame();
                if (identifier.empty() || containsZeroByte(identifier))
                    invalid("canonical template IR V3 identifier is noncanonical");
                if (identifier.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 identifier exceeds its byte limit");
                chargeBytes(
                    identifier.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                break;
            }
            case CanonicalIRNodeKind::SpecializedEnum: {
                const auto width = static_cast<SpecializedEnumWidth>(reader.readByte());
                if (!isKnown(width))
                    invalid("canonical template IR V3 Enum width is unknown");
                const auto entry_count = reader.readVarUInt();
                if (entry_count == 0)
                    invalid("canonical template IR V3 Enum is empty");
                if (entry_count > limits.maximum_ir_enum_entries || enum_entry_count > limits.maximum_ir_enum_entries - entry_count)
                    limit("canonical template IR V3 Enum entry count exceeds its limit");
                enum_entry_count += entry_count;
                bool has_previous = false;
                std::int64_t previous = 0;
                for (std::uint64_t index = 0; index < entry_count; ++index)
                {
                    const auto name = reader.readFrame();
                    chargeBytes(
                        name.size(),
                        literal_bytes,
                        limits.maximum_ir_literal_bytes,
                        "canonical template IR V3 literal bytes exceed their limit");
                    const auto value = decodeZigZag(reader.readVarUInt());
                    const bool in_range = width == SpecializedEnumWidth::Enum8
                        ? value >= std::numeric_limits<std::int8_t>::min() && value <= std::numeric_limits<std::int8_t>::max()
                        : value >= std::numeric_limits<std::int16_t>::min() && value <= std::numeric_limits<std::int16_t>::max();
                    if (!in_range)
                        invalid("canonical template IR V3 Enum value is out of range");
                    if (has_previous && value <= previous)
                        invalid("canonical template IR V3 Enum entries are not in canonical numeric order");
                    has_previous = true;
                    previous = value;
                }
                break;
            }
            case CanonicalIRNodeKind::FieldValue: {
                const auto field_kind = static_cast<CanonicalFieldKind>(reader.readByte());
                if (!isKnown(field_kind))
                    invalid("canonical template IR V3 Field kind is unknown");
                std::span<const Byte> payload;
                std::span<const Byte> state_name;
                switch (field_kind)
                {
                    case CanonicalFieldKind::None: invalid("canonical template IR V3 Field kind is absent");
                    case CanonicalFieldKind::Null:
                    case CanonicalFieldKind::NegativeInfinity:
                    case CanonicalFieldKind::PositiveInfinity: break;
                    case CanonicalFieldKind::String: payload = reader.readFrame(); break;
                    case CanonicalFieldKind::AggregateFunctionState:
                        state_name = reader.readFrame();
                        payload = reader.readFrame();
                        break;
                    case CanonicalFieldKind::Bool: payload = reader.readBytes(1); break;
                    case CanonicalFieldKind::IPv4: payload = reader.readBytes(4); break;
                    case CanonicalFieldKind::UInt64:
                    case CanonicalFieldKind::Int64:
                    case CanonicalFieldKind::Float64:
                    case CanonicalFieldKind::Decimal32: payload = reader.readBytes(8); break;
                    case CanonicalFieldKind::Decimal64: payload = reader.readBytes(12); break;
                    case CanonicalFieldKind::UInt128:
                    case CanonicalFieldKind::Int128:
                    case CanonicalFieldKind::UUID:
                    case CanonicalFieldKind::IPv6: payload = reader.readBytes(16); break;
                    case CanonicalFieldKind::Decimal128: payload = reader.readBytes(20); break;
                    case CanonicalFieldKind::UInt256:
                    case CanonicalFieldKind::Int256: payload = reader.readBytes(32); break;
                    case CanonicalFieldKind::Decimal256: payload = reader.readBytes(36); break;
                    case CanonicalFieldKind::Array:
                    case CanonicalFieldKind::Tuple:
                    case CanonicalFieldKind::Map:
                    case CanonicalFieldKind::Object: {
                        const auto child_count = reader.readVarUInt();
                        if (field_kind == CanonicalFieldKind::Map && (child_count % 2) != 0)
                            invalid("canonical template IR V3 Map Field has odd key/value arity");
                        chargeEdges(child_count);
                        std::span<const Byte> previous_object_key;
                        bool has_previous_object_key = false;
                        for (std::uint64_t index = 0; index < child_count; ++index)
                        {
                            if (field_kind == CanonicalFieldKind::Object)
                            {
                                const auto key = reader.readFrame();
                                chargeBytes(
                                    key.size(),
                                    literal_bytes,
                                    limits.maximum_ir_literal_bytes,
                                    "canonical template IR V3 literal bytes exceed their limit");
                                if (has_previous_object_key && compareBinarySpans(previous_object_key, key) >= 0)
                                    invalid("canonical template IR V3 Object Field keys are not in strict binary order");
                                previous_object_key = key;
                                has_previous_object_key = true;
                            }
                            validateTemplateReference(source, reader.readVarUInt());
                        }
                        break;
                    }
                }
                std::string_view state_name_view;
                if (!state_name.empty())
                    state_name_view = std::string_view(reinterpret_cast<const char *>(state_name.data()), state_name.size());
                validateCanonicalFieldPayload(field_kind, payload, state_name_view);
                chargeBytes(
                    payload.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                chargeBytes(
                    state_name.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                break;
            }
            case CanonicalIRNodeKind::AggregateFunction: {
                const auto name = reader.readFrame();
                if (name.empty() || containsZeroByte(name))
                    invalid("canonical template IR V3 aggregate-function name is noncanonical");
                if (name.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 aggregate-function name exceeds its byte limit");
                chargeBytes(
                    name.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                const auto action = static_cast<AggregateFunctionNullsAction>(reader.readByte());
                if (!isKnown(action))
                    invalid("canonical template IR V3 aggregate-function NULL action is unknown");
                const auto parameter_count = reader.readVarUInt();
                chargeEdges(parameter_count);
                for (std::uint64_t index = 0; index < parameter_count; ++index)
                    validateTemplateReference(source, reader.readVarUInt());
                break;
            }
            case CanonicalIRNodeKind::DynamicSetting:
            case CanonicalIRNodeKind::ObjectSetting: {
                const auto name = reader.readFrame();
                if (name.empty() || containsZeroByte(name))
                    invalid("canonical template IR V3 setting name is noncanonical");
                const std::string_view name_view(reinterpret_cast<const char *>(name.data()), name.size());
                if ((kind == CanonicalIRNodeKind::DynamicSetting && name_view != "max_types")
                    || (kind == CanonicalIRNodeKind::ObjectSetting && name_view != "max_dynamic_types"
                        && name_view != "max_dynamic_paths"))
                    invalid("canonical template IR V3 named setting is unknown");
                if (name.size() > limits.maximum_ir_identifier_bytes)
                    limit("canonical template IR V3 setting name exceeds its byte limit");
                chargeBytes(
                    name.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                chargeEdges(1);
                validateTemplateReference(source, reader.readVarUInt());
                break;
            }
            case CanonicalIRNodeKind::ObjectTypedPath: {
                const auto path = reader.readFrame();
                if (path.empty() || containsZeroByte(path))
                    invalid("canonical template IR V3 typed Object path is noncanonical");
                chargeBytes(
                    path.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                chargeEdges(1);
                validateTemplateReference(source, reader.readVarUInt());
                break;
            }
            case CanonicalIRNodeKind::ObjectSkipPath: {
                const auto path = reader.readFrame();
                if (path.empty() || containsZeroByte(path))
                    invalid("canonical template IR V3 skipped Object path is noncanonical");
                chargeBytes(
                    path.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                break;
            }
            case CanonicalIRNodeKind::ObjectSkipRegexp: {
                const auto regexp = reader.readFrame();
                chargeBytes(
                    regexp.size(),
                    literal_bytes,
                    limits.maximum_ir_literal_bytes,
                    "canonical template IR V3 literal bytes exceed their limit");
                break;
            }
        }
    }
    if (next_discovered_node != node_count)
        invalid("canonical template IR V3 contains an unreachable node");
    reader.requireEnd();
}

inline void validateSemanticDefinition(const SemanticDefinitionView & definition, const EncodingLimits & limits)
{
    if (definition.checker_abi == 0 || definition.checker_charge_abi == 0)
        invalid("checker ABI values must be nonzero");
    if (definition.formals.size() > limits.maximum_formals)
        limit("formal count exceeds its limit");
    for (std::size_t index = 0; index < definition.formals.size(); ++index)
    {
        const auto & formal = definition.formals[index];
        if (!isKnown(formal.kind) || formal.normalized_name.empty())
            invalid("formal declaration is invalid");
        if (formal.normalized_name.size() > limits.maximum_formal_name_bytes)
            limit("formal name exceeds its byte limit");
        /// Formal order is semantic, so duplicate detection is deliberately
        /// bounded by maximum_formals instead of reordering or allocating.
        for (std::size_t prior = 0; prior < index; ++prior)
        {
            if (definition.formals[prior].normalized_name == formal.normalized_name)
                invalid("formal names are not unique");
        }
    }
    if (definition.has_decreasing_parameter)
    {
        if (definition.decreasing_parameter >= definition.formals.size())
            invalid("decreasing formal ordinal is out of range");
        if (!isUnsigned(definition.formals[definition.decreasing_parameter].kind))
            invalid("decreasing formal is not unsigned");
    }
    else if (definition.decreasing_parameter != 0)
    {
        invalid("non-recursive semantic definition carries a hidden decreasing formal ordinal");
    }
    if (definition.policy_abi == 0 || definition.function_registry_abi == 0)
        invalid("policy and function-registry ABI values must be nonzero");
    if (definition.policy_bearing == (definition.policy_semantic_hash == empty_policy_semantic_hash))
        invalid("policy-bearing state disagrees with the canonical empty-policy marker");
    if (definition.canonical_template_ir.empty())
        invalid("canonical template IR is empty");
    if (definition.canonical_template_ir.size() > limits.maximum_template_ir_bytes)
        limit("canonical template IR exceeds its byte limit");
    validateSemanticDependencies(definition.direct_dependencies, limits);
}

inline void writeSemanticPayload(SpanWriter & writer, const SemanticDefinitionView & definition)
{
    writer.writeUInt16LE(encoding_version);
    writer.writeUInt16LE(definition.checker_abi);
    writer.writeUInt16LE(definition.checker_charge_abi);
    writer.writeVarUInt(static_cast<std::uint64_t>(definition.formals.size()));
    for (const auto & formal : definition.formals)
    {
        writer.writeByte(static_cast<Byte>(formal.kind));
        writer.writeFrame(formal.normalized_name);
    }
    writer.writeByte(definition.has_decreasing_parameter ? 1 : 0);
    if (definition.has_decreasing_parameter)
        writer.writeUInt16LE(definition.decreasing_parameter);
    writer.writeByte(definition.policy_bearing ? 1 : 0);
    writer.writeUInt16LE(definition.policy_abi);
    writer.writeUInt16LE(definition.function_registry_abi);
    writer.writeBytes(definition.policy_semantic_hash);
    writer.writeFrame(definition.canonical_template_ir);
    writeSemanticDependencies(writer, definition.direct_dependencies);
}

inline void validateProof(const CheckerProofView & proof, const EncodingLimits & limits)
{
    if (proof.checker_abi == 0 || proof.checker_charge_abi == 0)
        invalid("checker proof ABI values must be nonzero");
    if (proof.logical_node_count == 0)
        invalid("checker proof has no logical nodes");
    if (proof.logical_node_count > limits.maximum_logical_nodes)
        limit("checker proof logical-node count exceeds its limit");
    if (proof.self_call_occurrence_count > proof.logical_node_count)
        invalid("checker proof has more self-call occurrences than logical-node occurrences");
    if (proof.maximum_template_depth > limits.maximum_template_depth)
        limit("checker proof template depth exceeds its limit");
    if (proof.maximum_template_depth >= proof.logical_node_count)
        invalid("checker proof template depth is impossible for its logical-node count");
    if (proof.direct_dependency_count > limits.maximum_direct_dependencies
        || proof.transitive_dependency_count > limits.maximum_transitive_dependencies)
        limit("checker proof dependency count exceeds its limit");
    if (proof.dependency_proof_mode != DependencyProofMode::CompositionalClosure
        && proof.dependency_proof_mode != DependencyProofMode::ExactUniqueClosure)
        invalid("checker dependency-proof mode is unknown");
    switch (proof.dependency_proof_mode)
    {
        case DependencyProofMode::CompositionalClosure:
            if (proof.transitive_dependency_count != 0)
                invalid("compositional dependency proof must use the unmaterialized-count sentinel");
            break;
        case DependencyProofMode::ExactUniqueClosure:
            if (proof.direct_dependency_count > proof.transitive_dependency_count)
                invalid("direct dependency count exceeds exact transitive dependency count");
            break;
    }
    if (proof.checker_charge_units == 0)
        invalid("checker proof charge must be nonzero");
    if (proof.checker_charge_units > limits.maximum_checker_charge_units)
        limit("checker proof charge exceeds its limit");

    if (proof.recursion_mode != RecursionMode::Acyclic && proof.recursion_mode != RecursionMode::DecreasingSelf)
        invalid("checker proof recursion mode is unknown");
    if (!proof.has_measure
        && (proof.decreasing_parameter != 0 || proof.decreasing_parameter_kind != FormalKind::Type
            || proof.measure_rule != MeasureRule::ZeroGuardNMinusOneWithReachableBase))
        invalid("checker proof carries hidden recursive measure fields");
    switch (proof.recursion_mode)
    {
        case RecursionMode::Acyclic:
            if (proof.has_measure || proof.self_call_occurrence_count != 0)
                invalid("acyclic checker proof carries recursive measure state");
            break;
        case RecursionMode::DecreasingSelf:
            if (!proof.has_measure || proof.policy_bearing || proof.self_call_occurrence_count == 0
                || !isUnsigned(proof.decreasing_parameter_kind) || proof.measure_rule != MeasureRule::ZeroGuardNMinusOneWithReachableBase)
                invalid("decreasing checker proof does not satisfy its recursion contract");
            break;
    }
}

inline void writeProof(SpanWriter & writer, const CheckerProofView & proof)
{
    writer.writeUInt16LE(proof_version);
    writer.writeUInt16LE(proof.checker_abi);
    writer.writeUInt16LE(proof.checker_charge_abi);
    writer.writeBytes(proof.database_uuid);
    writer.writeBytes(proof.type_uuid);
    writer.writeUInt64LE(proof.revision);
    writer.writeBytes(proof.semantic_definition_digest);
    writer.writeByte(static_cast<Byte>(proof.recursion_mode));
    writer.writeByte(proof.policy_bearing ? 1 : 0);
    writer.writeByte(proof.has_measure ? 1 : 0);
    if (proof.has_measure)
    {
        writer.writeUInt16LE(proof.decreasing_parameter);
        writer.writeByte(static_cast<Byte>(proof.decreasing_parameter_kind));
        writer.writeByte(static_cast<Byte>(proof.measure_rule));
    }
    writer.writeVarUInt(proof.self_call_occurrence_count);
    writer.writeVarUInt(proof.logical_node_count);
    writer.writeVarUInt(proof.maximum_template_depth);
    writer.writeVarUInt(proof.direct_dependency_count);
    writer.writeBytes(proof.direct_dependency_digest);
    writer.writeByte(static_cast<Byte>(proof.dependency_proof_mode));
    writer.writeVarUInt(proof.transitive_dependency_count);
    writer.writeBytes(proof.transitive_dependency_closure_digest);
    writer.writeVarUInt(proof.checker_charge_units);
}

template <typename Validator, typename Encoder>
std::size_t encode(std::span<Byte> output, const EncodingLimits & limits, Validator && validator, Encoder && encoder)
{
    validator();
    SpanWriter writer(output, limits.maximum_output_bytes);
    encoder(writer);
    requireExactOutput(writer, output);
    return writer.size();
}

}

/// Passing an empty output span measures the exact size without allocating.
/// A nonempty output span must have exactly that measured size.
inline std::uint64_t checkerChargeUnits(
    std::uint64_t logical_node_count, std::uint64_t formal_count, std::uint64_t direct_dependency_count, const EncodingLimits & limits = {})
{
    if (logical_node_count == 0)
        detail::invalid("checker charge has no logical nodes");
    if (logical_node_count > limits.maximum_logical_nodes || formal_count > limits.maximum_formals
        || direct_dependency_count > limits.maximum_direct_dependencies)
        detail::limit("checker charge input exceeds its semantic limit");
    std::uint64_t result = logical_node_count;
    if (formal_count > limits.maximum_checker_charge_units || result > limits.maximum_checker_charge_units - formal_count)
        detail::limit("checker charge exceeds its unit limit");
    result += formal_count;
    if (direct_dependency_count > limits.maximum_checker_charge_units
        || result > limits.maximum_checker_charge_units - direct_dependency_count)
        detail::limit("checker charge exceeds its unit limit");
    return result + direct_dependency_count;
}

/// Validates one already-canonical Field leaf payload without allocating.
/// Structural composite-child validation is performed by the full IR
/// validator. This is shared by the production checker and V3 encoder so they
/// cannot drift on the closed Field-kind inventory.
inline void
validateCanonicalFieldValue(CanonicalFieldKind kind, std::span<const Byte> payload, std::string_view aggregate_state_name = {})
{
    detail::validateCanonicalFieldPayload(kind, payload, aggregate_state_name);
}

inline std::size_t
encodeLegacyCanonicalTemplateIR(const LegacyCanonicalTemplateIRView & canonical_ir, std::span<Byte> output = {}, EncodingLimits limits = {})
{
    limits.maximum_output_bytes = std::min(limits.maximum_output_bytes, limits.maximum_template_ir_bytes);
    return detail::encode(
        output,
        limits,
        [&] { detail::validateLegacyCanonicalTemplateIR(canonical_ir, limits); },
        [&](auto & writer) { detail::writeLegacyCanonicalTemplateIR(writer, canonical_ir); });
}

/// Passing an empty output span measures the exact V3 size without allocating.
/// A nonempty span must have exactly that size. This is deliberately separate
/// from encodeLegacyCanonicalTemplateIR(), whose V2 bytes are immutable.
inline std::size_t
encodeCanonicalTemplateIR(const CanonicalTemplateIRView & canonical_ir, std::span<Byte> output = {}, EncodingLimits limits = {})
{
    limits.maximum_output_bytes = std::min(limits.maximum_output_bytes, limits.maximum_template_ir_bytes);
    return detail::encode(
        output,
        limits,
        [&] { detail::validateCanonicalTemplateIR(canonical_ir, limits); },
        [&](auto & writer) { detail::writeCanonicalTemplateIR(writer, canonical_ir); });
}

/// Validates an untrusted V3 byte stream without allocating or accepting an
/// alternate spelling. In particular, overlong VarUInts, unknown tags,
/// malformed frames, non-dense discovery order, and trailing bytes fail.
///
/// This is only the allocation-free wire-canonicality boundary. A successful
/// return must never be treated as semantic admission or as a checked catalog
/// definition. Since canonical references point forward, proving a referenced
/// node's semantic kind requires retaining O(node_count) kinds (or repeatedly
/// rescanning the stream). Specialized-Enum label uniqueness, formal-kind
/// compatibility, built-in family shapes, and family-specific canonical order
/// likewise require checked-definition context or bounded scratch. The
/// production TemplateChecker proves those properties before encoding; the
/// structured V3 encoder additionally proves Field/Aggregate reference kinds.
/// Certificate admission must therefore require the TemplateChecker proof,
/// never this raw-wire validator alone.
inline void validateEncodedCanonicalTemplateIR(std::span<const Byte> input, const EncodingLimits & limits = {})
{
    detail::validateEncodedCanonicalTemplateIR(input, limits);
}

inline std::size_t
encodeSemanticPayload(const SemanticDefinitionView & definition, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [&] { detail::validateSemanticDefinition(definition, limits); },
        [&](auto & writer) { detail::writeSemanticPayload(writer, definition); });
}

inline std::size_t
semanticDigestPreimage(const SemanticDefinitionView & definition, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [&] { detail::validateSemanticDefinition(definition, limits); },
        [&](auto & writer)
        {
            detail::writeDomain(writer, semantic_definition_domain);
            detail::writeSemanticPayload(writer, definition);
        });
}

inline std::size_t
definitionHashPreimage(const Digest & semantic_definition_digest, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [] { },
        [&](auto & writer)
        {
            detail::writeDomain(writer, definition_hash_domain);
            writer.writeUInt16LE(encoding_version);
            writer.writeBytes(semantic_definition_digest);
        });
}

inline std::size_t
encodeDependencySet(std::span<const DependencyIdentity> dependencies, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [&] { detail::validateDependencies(dependencies, limits.maximum_direct_dependencies); },
        [&](auto & writer) { detail::writeDependencies(writer, dependencies); });
}

inline std::size_t dependencySetDigestPreimage(
    std::string_view domain,
    std::span<const DependencyIdentity> dependencies,
    std::span<Byte> output = {},
    const EncodingLimits & limits = {})
{
    if (domain != dependency_set_domain && domain != direct_dependency_set_domain && domain != transitive_dependency_set_domain)
        detail::invalid("dependency-set digest domain is unknown");
    return detail::encode(
        output,
        limits,
        [&]
        {
            const auto maximum
                = domain == direct_dependency_set_domain ? limits.maximum_direct_dependencies : limits.maximum_transitive_dependencies;
            detail::validateDependencies(dependencies, maximum);
        },
        [&](auto & writer)
        {
            detail::writeDomain(writer, domain);
            detail::writeDependencies(writer, dependencies);
        });
}

inline std::size_t compositionalClosureDigestPreimage(
    std::span<const CompositionalDependency> dependencies, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [&] { detail::validateCompositionalDependencies(dependencies, limits); },
        [&](auto & writer)
        {
            detail::writeDomain(writer, compositional_dependency_closure_domain);
            detail::writeCompositionalDependencies(writer, dependencies);
        });
}

inline std::size_t encodeCheckerProof(const CheckerProofView & proof, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output, limits, [&] { detail::validateProof(proof, limits); }, [&](auto & writer) { detail::writeProof(writer, proof); });
}

inline std::size_t
checkerProofDigestPreimage(const CheckerProofView & proof, std::span<Byte> output = {}, const EncodingLimits & limits = {})
{
    return detail::encode(
        output,
        limits,
        [&] { detail::validateProof(proof, limits); },
        [&](auto & writer)
        {
            detail::writeDomain(writer, checker_proof_domain);
            detail::writeProof(writer, proof);
        });
}

}
