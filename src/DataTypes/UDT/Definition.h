#pragma once

#include <DataTypes/UDT/TemplateCheckerCertificateEncoding.h>

#include <Core/Types.h>
#include <Core/UUID.h>

#include <memory>
#include <optional>
#include <span>
#include <vector>

namespace DB
{
class Field;
}

namespace DB::UDT
{

using Digest = CheckerProof::Digest;
using TemplateNodeID = UInt32;

/// Values are part of the canonical V1 ABI. Do not reorder or serialize the
/// compiler enum ordinal without going through toCheckerProofKind().
enum class ParameterKind : UInt8
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

CheckerProof::FormalKind toCheckerProofKind(ParameterKind kind);

bool isUnsignedIntegerParameter(ParameterKind kind) noexcept;
bool isSignedIntegerParameter(ParameterKind kind) noexcept;
bool isIntegerParameter(ParameterKind kind) noexcept;

struct DefinitionIdentity
{
    UUID database_uuid = UUIDHelpers::Nil;
    UUID type_uuid = UUIDHelpers::Nil;
    UInt64 revision = 0;

    bool operator==(const DefinitionIdentity &) const = default;
};

struct Parameter
{
    String normalized_name;
    ParameterKind kind = ParameterKind::Type;

    bool operator==(const Parameter &) const = default;
};

enum class TemplateNodeKind : UInt8
{
    BuiltIn,
    TypeParameter,
    ValueParameter,
    UnsignedLiteral,
    BooleanLiteral,
    SignedLiteral,
    StringLiteral,
    Identifier,
    SpecializedEnum,
    TypeIfZero,
    SelfCall,
    DefinitionCall,
    FieldValue,
    AggregateFunction,
    DynamicSetting,
    ObjectSetting,
    ObjectTypedPath,
    ObjectSkipPath,
    ObjectSkipRegexp,
};

/// Local model enum. Wire values are mapped explicitly to
/// CheckerProof::CanonicalFieldKind and never serialized by ordinal.
enum class CanonicalFieldKind : UInt8
{
    None,
    Null,
    UInt64,
    Int64,
    Float64,
    String,
    Bool,
    UInt128,
    Int128,
    UInt256,
    Int256,
    Decimal32,
    Decimal64,
    Decimal128,
    Decimal256,
    UUID,
    IPv4,
    IPv6,
    NegativeInfinity,
    PositiveInfinity,
    Array,
    Tuple,
    Map,
    Object,
    AggregateFunctionState,
};

struct CanonicalFieldValue
{
    CanonicalFieldKind kind = CanonicalFieldKind::None;
    /// Value-preserving canonical bytes. Numeric, decimal and IP payloads are
    /// little-endian; UUID is RFC textual order; String is binary-safe; Null
    /// has no payload. Decimal payloads append a UInt32 little-endian scale.
    String payload;
    /// Raw name for AggregateFunctionState; empty for all other kinds.
    String name;

    static CanonicalFieldValue fromField(const Field & field);

    bool operator==(const CanonicalFieldValue &) const = default;
};

enum class AggregateFunctionNullsAction : UInt8
{
    Empty,
    RespectNulls,
    IgnoreNulls,
};

enum class SpecializedEnumWidth : UInt8
{
    None = 0,
    Enum8 = 1,
    Enum16 = 2,
};

struct SpecializedEnumEntry
{
    /// Raw binary-safe label after parser literal decoding.
    String name;
    Int64 value = 0;

    bool operator==(const SpecializedEnumEntry &) const = default;
};

struct TemplateNodeChild
{
    /// A template-node ID for a built-in type and TypeIfZero, or a caller-parameter
    /// ordinal for DefinitionCall.
    TemplateNodeID reference = 0;
    /// Only built-in type edges may carry a physical Tuple field label.
    String label;

    bool operator==(const TemplateNodeChild &) const = default;
};

struct TemplateNode
{
    TemplateNodeKind kind = TemplateNodeKind::BuiltIn;
    /// Built-in family name. Empty for every other node kind.
    String atom;
    UInt16 parameter = 0;
    UInt64 decrement = 0;
    UInt64 unsigned_literal = 0;
    Int64 signed_literal = 0;
    bool boolean_literal = false;
    /// Raw payload for StringLiteral or normalized spelling for Identifier.
    String text;
    SpecializedEnumWidth specialized_enum_width = SpecializedEnumWidth::None;
    std::vector<SpecializedEnumEntry> enum_entries;
    CanonicalFieldValue field_value;
    AggregateFunctionNullsAction aggregate_nulls_action = AggregateFunctionNullsAction::Empty;
    /// Stable ordinal in DefinitionInput::dependencies.
    UInt16 dependency_ordinal = 0;
    std::vector<TemplateNodeChild> children;

    bool operator==(const TemplateNode &) const = default;
};

struct CanonicalFieldValueLimits
{
    UInt64 maximum_nodes = 1ULL << 20;
    UInt64 maximum_edges = 1ULL << 22;
    UInt64 maximum_entries = 1ULL << 20;
    UInt64 maximum_depth = 256;
    UInt64 maximum_literal_bytes = 1ULL << 26;
};

/// Appends one or more complete Field-value graphs in canonical
/// first-discovery order and returns their root node IDs. Composite values use
/// structural child edges; CustomType is rejected because FieldBinaryEncoding
/// cannot round-trip it either. Existing nodes are left untouched on failure.
std::vector<TemplateNodeID> appendCanonicalFieldValues(
    std::span<const Field> fields, std::vector<TemplateNode> & nodes, CanonicalFieldValueLimits limits = {});

struct DefinitionDependency
{
    /// V1 durable definitions can depend only on the same database authority,
    /// so the semantic identity does not repeat database_uuid.
    UUID type_uuid = UUIDHelpers::Nil;
    UInt64 revision = 0;
    Digest target_definition_hash{};

    bool operator==(const DefinitionDependency &) const = default;
};

enum class SemanticCapability : UInt8
{
    Input = 1U << 0,
    Output = 1U << 1,
    ValueChecks = 1U << 2,
    Default = 1U << 3,
};

using SemanticCapabilityMask = UInt8;

constexpr SemanticCapabilityMask semanticCapabilityBit(SemanticCapability capability) noexcept
{
    return static_cast<SemanticCapabilityMask>(capability);
}

constexpr bool hasSemanticCapability(SemanticCapabilityMask mask, SemanticCapability capability) noexcept
{
    return (mask & semanticCapabilityBit(capability)) != 0;
}

inline constexpr SemanticCapabilityMask all_semantic_capabilities = semanticCapabilityBit(SemanticCapability::Input)
    | semanticCapabilityBit(SemanticCapability::Output) | semanticCapabilityBit(SemanticCapability::ValueChecks)
    | semanticCapabilityBit(SemanticCapability::Default);

/// Borrowed-at-the-API-boundary input. TemplateChecker validates and copies it
/// into an immutable definition; no reference or string_view survives.
struct DefinitionInput
{
    DefinitionIdentity identity;
    /// Diagnostic lookup/display name, optionally database-qualified. It is
    /// normalized before this boundary and deliberately does not participate
    /// in definition_hash.
    String normalized_name;
    /// Exact normalized local identifier supplied by the structured name
    /// boundary. Qualified names must set it explicitly; an unqualified name
    /// may leave it empty and is then its own local identifier. Never derive
    /// this value by splitting formatted SQL: quoted identifiers may contain
    /// dots. Built-in no-shadow admission is applied to this token.
    String normalized_local_name;
    std::vector<Parameter> parameters;
    std::optional<UInt16> decreasing_parameter;
    std::vector<TemplateNode> nodes;
    TemplateNodeID root = 0;
    bool policy_bearing = false;
    /// Precomputed from the checked definition's executable semantic clauses.
    /// Merely being a declared UDT occurrence does not set any capability.
    SemanticCapabilityMask semantic_capabilities = 0;
    UInt16 checker_abi = 1;
    UInt16 checker_charge_abi = 1;
    UInt16 policy_abi = 1;
    UInt16 function_registry_abi = 1;
    Digest policy_semantic_hash = CheckerProof::empty_policy_semantic_hash;
    std::vector<DefinitionDependency> dependencies;
};

struct TemplateCheckerCertificate
{
    String canonical_template_ir;
    Digest semantic_definition_digest{};
    Digest definition_hash{};
    Digest compositional_dependency_closure_digest{};
    String encoded_certificate;
    Digest certificate_digest{};
    UInt64 charged_work = 0;
    UInt64 logical_node_count = 0;
    UInt64 maximum_template_depth = 0;
    /// Runtime-only exact size of this definition's unique same-database
    /// transitive dependency closure. The durable compositional V1 proof
    /// deliberately keeps its historical zero sentinel; recovery recomputes
    /// this value from the complete checked graph before quota admission.
    UInt64 transitive_dependency_count = 0;

    bool operator==(const TemplateCheckerCertificate &) const = default;
};

class TemplateChecker;

/// Immutable catalog payload. Construction is owned exclusively by the
/// checker, and consumers receive shared_ptr<const ...>. It contains neither
/// Context nor a catalog-generation owner and is safe to retain in bound
/// schema metadata after the source catalog root is retired.
class Definition final
{
public:
    using Ptr = std::shared_ptr<const Definition>;

    Definition(const Definition &) = delete;
    Definition & operator=(const Definition &) = delete;
    Definition(Definition &&) = delete;
    Definition & operator=(Definition &&) = delete;

    const DefinitionIdentity & getIdentity() const noexcept { return identity; }
    const String & getNormalizedName() const noexcept { return normalized_name; }
    const String & getNormalizedLocalName() const noexcept { return normalized_local_name; }
    const std::vector<Parameter> & getParameters() const noexcept { return parameters; }
    const std::optional<UInt16> & getDecreasingParameter() const noexcept { return decreasing_parameter; }
    const std::vector<TemplateNode> & getNodes() const noexcept { return nodes; }
    TemplateNodeID getRoot() const noexcept { return root; }
    bool isPolicyBearing() const noexcept { return policy_bearing; }
    SemanticCapabilityMask getSemanticCapabilities() const noexcept { return semantic_capabilities; }
    UInt16 getCheckerABI() const noexcept { return checker_abi; }
    UInt16 getCheckerChargeABI() const noexcept { return checker_charge_abi; }
    UInt16 getPolicyABI() const noexcept { return policy_abi; }
    UInt16 getFunctionRegistryABI() const noexcept { return function_registry_abi; }
    const Digest & getPolicySemanticHash() const noexcept { return policy_semantic_hash; }
    const std::vector<DefinitionDependency> & getDependencies() const noexcept { return dependencies; }
    const TemplateCheckerCertificate & getCertificate() const noexcept { return certificate; }
    const Digest & getDefinitionHash() const noexcept { return certificate.definition_hash; }

    /// Exact checked-semantic equality for one immutable identity. The
    /// diagnostic normalized name is deliberately excluded so an authority
    /// can rename a definition without changing its identity or executable
    /// meaning. No digest is trusted as a substitute for comparing the full
    /// checked body and certificate.
    bool hasSameCheckedSemantics(const Definition & other) const noexcept;

private:
    friend class TemplateChecker;

    Definition(DefinitionInput input, TemplateCheckerCertificate certificate_);

    DefinitionIdentity identity;
    String normalized_local_name;
    String normalized_name;
    std::vector<Parameter> parameters;
    std::optional<UInt16> decreasing_parameter;
    std::vector<TemplateNode> nodes;
    TemplateNodeID root;
    bool policy_bearing;
    SemanticCapabilityMask semantic_capabilities;
    UInt16 checker_abi;
    UInt16 checker_charge_abi;
    UInt16 policy_abi;
    UInt16 function_registry_abi;
    Digest policy_semantic_hash;
    std::vector<DefinitionDependency> dependencies;
    TemplateCheckerCertificate certificate;
};

/// Returns the exact allocator-independent logical footprint retained by one
/// immutable definition, or nullopt when it exceeds maximum_bytes (including
/// UInt64 arithmetic overflow). The count includes the definition object,
/// every vector element, and every owned string payload. It performs no
/// allocation and is the single accounting contract shared by authority
/// publication and specialization-time admission.
std::optional<UInt64> tryCountLogicalRetainedDefinitionBytes(const Definition & definition, UInt64 maximum_bytes) noexcept;

}
