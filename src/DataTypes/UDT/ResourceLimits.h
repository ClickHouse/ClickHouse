#pragma once

#include <Core/Types.h>

#include <array>
#include <bitset>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>

namespace DB::UDT
{

inline constexpr UInt16 resource_limit_contract_abi = 2;
/// Frozen upper bound shared by every durable catalog decoder and immutable
/// authority-root boundary. Mutable policy layers may only select a lower
/// value.
inline constexpr UInt64 resource_implementation_maximum_deterministic_catalog_bytes = 1ULL << 30;
inline constexpr UInt64 resource_implementation_maximum_durable_dependent_object_bytes = 1ULL << 30;

/// Every entry is an independent finite ceiling. Values are stable diagnostic
/// identities: append new entries, but never reorder an existing one.
enum class ResourceLimit : UInt16
{
    CanonicalDefinitionBytes,
    TemplateDepth,
    LogicalTemplateNodes,
    LoweredPhysicalTypeNodes,
    FormalParameters,
    CanonicalActualArgumentBytes,
    DirectDependencies,
    TransitiveDependencies,
    PersistedSpecializationsPerTemplate,
    CheckerExpansionWorkUnits,

    DefinitionsPerDatabase,
    DeterministicCatalogBytesPerDatabase,
    PhysicalizationTokensPerDatabase,
    PhysicalizationTokenBytesPerDatabase,
    PhysicalizationTokensPerPrincipalDatabase,
    PhysicalizationTokenBytesPerPrincipalDatabase,
    PhysicalizationTokenBytesPerRecord,
    PhysicalizationTokenTTLMicroseconds,

    OccurrencePathsPerObject,
    SidecarBytesPerObject,

    SemanticSinksPerQuery,
    DenseRoleRecordsPerQuery,
    DemandedDescriptorsPerQuery,
    InternedLogicalShapesPerQuery,
    NodePathStatesPerQuery,
    InspectedEdgesPerQuery,
    LogicalPathDepth,
    ExplicitNamesPerQuery,
    ContextualLiteralBytesPerQuery,
    SemanticScratchBytesPerQuery,
    ConflictSamplesPerQuery,
    DiagnosticBytesPerQuery,

    ManifestEntriesPerFragment,
    ManifestBytesPerFragment,
    ManifestDispatchBytesPerAttempt,

    LiveCatalogAndCacheBytesPerServer,

    VerificationTargetsPerDatabase,
    VerificationBucketsPerDatabase,
    VerificationTargetsPerBatch,
    VerificationCanonicalBytesPerBatch,
    VerificationWorkUnitsPerBatch,
    VerificationTransientBytesPerBatch,
    VerificationIOBytesPerBatch,
    VerificationPlannerWorkUnitsPerBatch,
    VerificationPlannerScratchBytesPerBatch,
    VerificationRetainedBytesPerBatch,

    DurableDependentObjectBytesPerDatabase,

    Count,
};

inline constexpr std::size_t resource_limit_count = static_cast<std::size_t>(ResourceLimit::Count);

std::string_view resourceLimitName(ResourceLimit limit) noexcept;

enum class ResourceLimitLayerKind : UInt8
{
    Implementation,
    Server,
    Database,
    QueryProfile,
    AuthorityAdapter,
    RemoteCapability,
};

std::string_view resourceLimitLayerName(ResourceLimitLayerKind layer) noexcept;

class ResourceLimitError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidLimit,
        InvalidLayer,
        ZeroLimit,
        ExceedsImplementationMaximum,
        InvalidDetectedMemory,
    };

    ResourceLimitError(Code code_, std::string_view message);

    const Code code;
};

/// Complete immutable implementation maxima. There is deliberately no zero,
/// `UINT64_MAX`, or other unlimited sentinel.
class ResourceLimits final
{
public:
    UInt64 get(ResourceLimit limit) const noexcept;

    bool operator==(const ResourceLimits &) const = default;

private:
    explicit constexpr ResourceLimits(std::array<UInt64, resource_limit_count> values_)
        : values(values_)
    {
    }

    friend const ResourceLimits & getResourceImplementationLimits() noexcept;
    friend class EffectiveResourceLimits;

    std::array<UInt64, resource_limit_count> values;
};

/// A layer contains only limits applicable at that boundary. Absence means
/// "not applicable", never "unlimited". Every present value must be nonzero.
class ResourceLimitLayer final
{
public:
    explicit ResourceLimitLayer(ResourceLimitLayerKind kind_);

    ResourceLimitLayer & set(ResourceLimit limit, UInt64 value);
    bool contains(ResourceLimit limit) const noexcept;
    std::optional<UInt64> get(ResourceLimit limit) const noexcept;
    ResourceLimitLayerKind getKind() const noexcept { return kind; }

private:
    ResourceLimitLayerKind kind;
    std::array<UInt64, resource_limit_count> values{};
    std::bitset<resource_limit_count> applicable;
};

/// The implementation layer is always present implicitly. Each applicable
/// configured/negotiated layer can only retain or lower its finite maximum.
class EffectiveResourceLimits final
{
public:
    UInt64 get(ResourceLimit limit) const noexcept;
    ResourceLimitLayerKind getBindingLayer(ResourceLimit limit) const noexcept;

    bool operator==(const EffectiveResourceLimits &) const = default;

private:
    EffectiveResourceLimits() = default;

    friend EffectiveResourceLimits calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer> layers);
    friend EffectiveResourceLimits
    minimumEffectiveResourceLimits(const EffectiveResourceLimits & lhs, const EffectiveResourceLimits & rhs) noexcept;

    std::array<UInt64, resource_limit_count> values{};
    std::array<ResourceLimitLayerKind, resource_limit_count> binding_layers{};
};

/// Finite implementation/decoder maxima for this contract ABI.
const ResourceLimits & getResourceImplementationLimits() noexcept;

/// Normative selected defaults. The server layer covers every counter. The
/// database and query layers contain only counters owned by those boundaries.
ResourceLimitLayer makeServerDefaultResourceLimitLayer(UInt64 detected_memory_bytes);
ResourceLimitLayer makeDatabaseDefaultResourceLimitLayer();
ResourceLimitLayer makeQueryDefaultResourceLimitLayer();

/// Computes the element-wise minimum across the immutable implementation
/// maxima and every applicable layer. A value above the implementation domain
/// is rejected rather than silently clamped.
EffectiveResourceLimits calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer> layers);

/// Element-wise minimum of two already-validated effective tuples. Binding
/// provenance follows the selected value; equal values retain the more
/// specific layer. This operation cannot widen either input.
EffectiveResourceLimits minimumEffectiveResourceLimits(const EffectiveResourceLimits & lhs, const EffectiveResourceLimits & rhs) noexcept;

}
