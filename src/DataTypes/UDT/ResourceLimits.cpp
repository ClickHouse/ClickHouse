#include <DataTypes/UDT/ResourceLimits.h>

#include <algorithm>
#include <exception>

namespace DB::UDT
{
namespace
{

constexpr UInt64 KiB = 1ULL << 10;
constexpr UInt64 MiB = 1ULL << 20;
constexpr UInt64 GiB = 1ULL << 30;
constexpr UInt64 maximum_token_ttl_microseconds = 15ULL * 60 * 1'000'000;

constexpr std::size_t indexOf(ResourceLimit limit) noexcept
{
    return static_cast<std::size_t>(limit);
}

std::size_t checkedIndexOf(ResourceLimit limit) noexcept
{
    const auto index = indexOf(limit);
    if (index >= resource_limit_count)
        std::terminate();
    return index;
}

bool isKnownLimit(ResourceLimit limit) noexcept
{
    return indexOf(limit) < resource_limit_count;
}

bool isKnownLayer(ResourceLimitLayerKind layer) noexcept
{
    return layer >= ResourceLimitLayerKind::Implementation && layer <= ResourceLimitLayerKind::RemoteCapability;
}

UInt8 layerPriority(ResourceLimitLayerKind layer) noexcept
{
    return static_cast<UInt8>(layer);
}

ResourceLimitLayer makeDefinitionDefaultLayer(ResourceLimitLayerKind kind)
{
    ResourceLimitLayer result(kind);
    result.set(ResourceLimit::CanonicalDefinitionBytes, 256 * KiB)
        .set(ResourceLimit::TemplateDepth, 32)
        .set(ResourceLimit::LogicalTemplateNodes, 4'096)
        .set(ResourceLimit::LoweredPhysicalTypeNodes, 1'000)
        .set(ResourceLimit::FormalParameters, 64)
        .set(ResourceLimit::CanonicalActualArgumentBytes, 64 * KiB)
        .set(ResourceLimit::DirectDependencies, 256)
        .set(ResourceLimit::TransitiveDependencies, 1'024)
        .set(ResourceLimit::PersistedSpecializationsPerTemplate, 4'096)
        .set(ResourceLimit::CheckerExpansionWorkUnits, 65'536);
    return result;
}

void addDatabaseDefaults(ResourceLimitLayer & result)
{
    result.set(ResourceLimit::DefinitionsPerDatabase, 10'000)
        .set(ResourceLimit::DeterministicCatalogBytesPerDatabase, 256 * MiB)
        .set(ResourceLimit::PhysicalizationTokensPerDatabase, 1'024)
        .set(ResourceLimit::PhysicalizationTokenBytesPerDatabase, 1 * MiB)
        .set(ResourceLimit::PhysicalizationTokensPerPrincipalDatabase, 64)
        .set(ResourceLimit::PhysicalizationTokenBytesPerPrincipalDatabase, 256 * KiB)
        .set(ResourceLimit::PhysicalizationTokenBytesPerRecord, 4 * KiB)
        .set(ResourceLimit::PhysicalizationTokenTTLMicroseconds, maximum_token_ttl_microseconds)
        .set(ResourceLimit::OccurrencePathsPerObject, 65'536)
        .set(ResourceLimit::SidecarBytesPerObject, 16 * MiB)
        .set(ResourceLimit::DurableDependentObjectBytesPerDatabase, 1 * GiB);
}

void addQueryDefaults(ResourceLimitLayer & result)
{
    result.set(ResourceLimit::SemanticSinksPerQuery, 16'384)
        .set(ResourceLimit::DenseRoleRecordsPerQuery, 16'384)
        .set(ResourceLimit::DemandedDescriptorsPerQuery, 16'384)
        .set(ResourceLimit::InternedLogicalShapesPerQuery, 16'384)
        .set(ResourceLimit::NodePathStatesPerQuery, 131'072)
        .set(ResourceLimit::InspectedEdgesPerQuery, 524'288)
        .set(ResourceLimit::LogicalPathDepth, 64)
        .set(ResourceLimit::ExplicitNamesPerQuery, 4'096)
        .set(ResourceLimit::ContextualLiteralBytesPerQuery, 1 * MiB)
        .set(ResourceLimit::SemanticScratchBytesPerQuery, 64 * MiB)
        .set(ResourceLimit::ConflictSamplesPerQuery, 32)
        .set(ResourceLimit::DiagnosticBytesPerQuery, 64 * KiB)
        .set(ResourceLimit::ManifestEntriesPerFragment, 16'384)
        .set(ResourceLimit::ManifestBytesPerFragment, 4 * MiB)
        .set(ResourceLimit::ManifestDispatchBytesPerAttempt, 16 * MiB);
}

void addVerificationDefaults(ResourceLimitLayer & result)
{
    result.set(ResourceLimit::VerificationTargetsPerDatabase, 200'000)
        .set(ResourceLimit::VerificationBucketsPerDatabase, 256)
        .set(ResourceLimit::VerificationTargetsPerBatch, 1'024)
        .set(ResourceLimit::VerificationCanonicalBytesPerBatch, 256 * MiB)
        .set(ResourceLimit::VerificationWorkUnitsPerBatch, 8'388'608)
        .set(ResourceLimit::VerificationTransientBytesPerBatch, 64 * MiB)
        .set(ResourceLimit::VerificationIOBytesPerBatch, 256 * MiB)
        .set(ResourceLimit::VerificationPlannerWorkUnitsPerBatch, 67'108'864)
        .set(ResourceLimit::VerificationPlannerScratchBytesPerBatch, 16 * MiB)
        .set(ResourceLimit::VerificationRetainedBytesPerBatch, 16 * MiB);
}

}

std::string_view resourceLimitName(ResourceLimit limit) noexcept
{
    switch (limit)
    {
        case ResourceLimit::CanonicalDefinitionBytes: return "canonical_definition_bytes";
        case ResourceLimit::TemplateDepth: return "template_depth";
        case ResourceLimit::LogicalTemplateNodes: return "logical_template_nodes";
        case ResourceLimit::LoweredPhysicalTypeNodes: return "lowered_physical_type_nodes";
        case ResourceLimit::FormalParameters: return "formal_parameters";
        case ResourceLimit::CanonicalActualArgumentBytes: return "canonical_actual_argument_bytes";
        case ResourceLimit::DirectDependencies: return "direct_dependencies";
        case ResourceLimit::TransitiveDependencies: return "transitive_dependencies";
        case ResourceLimit::PersistedSpecializationsPerTemplate: return "persisted_specializations_per_template";
        case ResourceLimit::CheckerExpansionWorkUnits: return "checker_expansion_work_units";
        case ResourceLimit::DefinitionsPerDatabase: return "definitions_per_database";
        case ResourceLimit::DeterministicCatalogBytesPerDatabase: return "deterministic_catalog_bytes_per_database";
        case ResourceLimit::PhysicalizationTokensPerDatabase: return "physicalization_tokens_per_database";
        case ResourceLimit::PhysicalizationTokenBytesPerDatabase: return "physicalization_token_bytes_per_database";
        case ResourceLimit::PhysicalizationTokensPerPrincipalDatabase: return "physicalization_tokens_per_principal_database";
        case ResourceLimit::PhysicalizationTokenBytesPerPrincipalDatabase: return "physicalization_token_bytes_per_principal_database";
        case ResourceLimit::PhysicalizationTokenBytesPerRecord: return "physicalization_token_bytes_per_record";
        case ResourceLimit::PhysicalizationTokenTTLMicroseconds: return "physicalization_token_ttl_microseconds";
        case ResourceLimit::OccurrencePathsPerObject: return "occurrence_paths_per_object";
        case ResourceLimit::SidecarBytesPerObject: return "sidecar_bytes_per_object";
        case ResourceLimit::SemanticSinksPerQuery: return "semantic_sinks_per_query";
        case ResourceLimit::DenseRoleRecordsPerQuery: return "dense_role_records_per_query";
        case ResourceLimit::DemandedDescriptorsPerQuery: return "demanded_descriptors_per_query";
        case ResourceLimit::InternedLogicalShapesPerQuery: return "interned_logical_shapes_per_query";
        case ResourceLimit::NodePathStatesPerQuery: return "node_path_states_per_query";
        case ResourceLimit::InspectedEdgesPerQuery: return "inspected_edges_per_query";
        case ResourceLimit::LogicalPathDepth: return "logical_path_depth";
        case ResourceLimit::ExplicitNamesPerQuery: return "explicit_names_per_query";
        case ResourceLimit::ContextualLiteralBytesPerQuery: return "contextual_literal_bytes_per_query";
        case ResourceLimit::SemanticScratchBytesPerQuery: return "semantic_scratch_bytes_per_query";
        case ResourceLimit::ConflictSamplesPerQuery: return "conflict_samples_per_query";
        case ResourceLimit::DiagnosticBytesPerQuery: return "diagnostic_bytes_per_query";
        case ResourceLimit::ManifestEntriesPerFragment: return "manifest_entries_per_fragment";
        case ResourceLimit::ManifestBytesPerFragment: return "manifest_bytes_per_fragment";
        case ResourceLimit::ManifestDispatchBytesPerAttempt: return "manifest_dispatch_bytes_per_attempt";
        case ResourceLimit::LiveCatalogAndCacheBytesPerServer: return "live_catalog_and_cache_bytes_per_server";
        case ResourceLimit::VerificationTargetsPerDatabase: return "verification_targets_per_database";
        case ResourceLimit::VerificationBucketsPerDatabase: return "verification_buckets_per_database";
        case ResourceLimit::VerificationTargetsPerBatch: return "verification_targets_per_batch";
        case ResourceLimit::VerificationCanonicalBytesPerBatch: return "verification_canonical_bytes_per_batch";
        case ResourceLimit::VerificationWorkUnitsPerBatch: return "verification_work_units_per_batch";
        case ResourceLimit::VerificationTransientBytesPerBatch: return "verification_transient_bytes_per_batch";
        case ResourceLimit::VerificationIOBytesPerBatch: return "verification_io_bytes_per_batch";
        case ResourceLimit::VerificationPlannerWorkUnitsPerBatch: return "verification_planner_work_units_per_batch";
        case ResourceLimit::VerificationPlannerScratchBytesPerBatch: return "verification_planner_scratch_bytes_per_batch";
        case ResourceLimit::VerificationRetainedBytesPerBatch: return "verification_retained_bytes_per_batch";
        case ResourceLimit::DurableDependentObjectBytesPerDatabase: return "durable_dependent_object_bytes_per_database";
        case ResourceLimit::Count: break;
    }
    return "unknown";
}

std::string_view resourceLimitLayerName(ResourceLimitLayerKind layer) noexcept
{
    switch (layer)
    {
        case ResourceLimitLayerKind::Implementation: return "implementation";
        case ResourceLimitLayerKind::Server: return "server";
        case ResourceLimitLayerKind::Database: return "database";
        case ResourceLimitLayerKind::QueryProfile: return "query_profile";
        case ResourceLimitLayerKind::AuthorityAdapter: return "authority_adapter";
        case ResourceLimitLayerKind::RemoteCapability: return "remote_capability";
    }
    return "unknown";
}

ResourceLimitError::ResourceLimitError(Code code_, std::string_view message)
    : std::runtime_error(String(message))
    , code(code_)
{
}

UInt64 ResourceLimits::get(ResourceLimit limit) const noexcept
{
    return values[checkedIndexOf(limit)];
}

ResourceLimitLayer::ResourceLimitLayer(ResourceLimitLayerKind kind_)
    : kind(kind_)
{
    if (!isKnownLayer(kind) || kind == ResourceLimitLayerKind::Implementation)
        throw ResourceLimitError(ResourceLimitError::Code::InvalidLayer, "resource limit override layer is invalid");
}

ResourceLimitLayer & ResourceLimitLayer::set(ResourceLimit limit, UInt64 value)
{
    if (!isKnownLimit(limit))
        throw ResourceLimitError(ResourceLimitError::Code::InvalidLimit, "resource limit identity is invalid");
    if (value == 0)
    {
        String message = "resource limit '";
        message.append(resourceLimitName(limit));
        message.append("' must be nonzero");
        throw ResourceLimitError(ResourceLimitError::Code::ZeroLimit, message);
    }
    const auto index = indexOf(limit);
    values[index] = value;
    applicable.set(index);
    return *this;
}

bool ResourceLimitLayer::contains(ResourceLimit limit) const noexcept
{
    return isKnownLimit(limit) && applicable.test(indexOf(limit));
}

std::optional<UInt64> ResourceLimitLayer::get(ResourceLimit limit) const noexcept
{
    if (!contains(limit))
        return std::nullopt;
    return values[indexOf(limit)];
}

UInt64 EffectiveResourceLimits::get(ResourceLimit limit) const noexcept
{
    return values[checkedIndexOf(limit)];
}

ResourceLimitLayerKind EffectiveResourceLimits::getBindingLayer(ResourceLimit limit) const noexcept
{
    return binding_layers[checkedIndexOf(limit)];
}

const ResourceLimits & getResourceImplementationLimits() noexcept
{
    static constexpr ResourceLimits limits({
        256 * KiB,
        32,
        4'096,
        1'000,
        64,
        64 * KiB,
        256,
        1'024,
        4'096,
        65'536,

        100'000,
        resource_implementation_maximum_deterministic_catalog_bytes,
        1'024,
        1 * MiB,
        64,
        256 * KiB,
        4 * KiB,
        maximum_token_ttl_microseconds,

        65'536,
        16 * MiB,

        16'384,
        16'384,
        16'384,
        16'384,
        131'072,
        524'288,
        64,
        4'096,
        1 * MiB,
        64 * MiB,
        32,
        64 * KiB,

        16'384,
        4 * MiB,
        16 * MiB,

        1 * GiB,

        200'000,
        4'096,
        1'024,
        256 * MiB,
        8'388'608,
        64 * MiB,
        256 * MiB,
        67'108'864,
        16 * MiB,
        16 * MiB,

        resource_implementation_maximum_durable_dependent_object_bytes,
    });
    static_assert(
        [](const auto & values)
        {
            for (const auto value : values)
                if (value == 0)
                    return false;
            return true;
        }(limits.values));
    return limits;
}

ResourceLimitLayer makeServerDefaultResourceLimitLayer(UInt64 detected_memory_bytes)
{
    if (detected_memory_bytes < 20)
        throw ResourceLimitError(
            ResourceLimitError::Code::InvalidDetectedMemory, "detected memory is too small to derive a nonzero UDT live-state limit");

    /// The server layer is a process policy, not a duplicate of normative
    /// database/query defaults. Its unconfigured value must leave those lower
    /// layers free to select (and a persisted Database replacement free to
    /// raise) their own values up to the implementation ceiling.
    ResourceLimitLayer result(ResourceLimitLayerKind::Server);
    const auto & implementation = getResourceImplementationLimits();
    for (size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        result.set(limit, implementation.get(limit));
    }
    result.set(ResourceLimit::LiveCatalogAndCacheBytesPerServer, std::min(1 * GiB, detected_memory_bytes / 20));
    return result;
}

ResourceLimitLayer makeDatabaseDefaultResourceLimitLayer()
{
    auto result = makeDefinitionDefaultLayer(ResourceLimitLayerKind::Database);
    addDatabaseDefaults(result);
    addVerificationDefaults(result);
    return result;
}

ResourceLimitLayer makeQueryDefaultResourceLimitLayer()
{
    auto result = makeDefinitionDefaultLayer(ResourceLimitLayerKind::QueryProfile);
    addQueryDefaults(result);
    return result;
}

EffectiveResourceLimits calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer> layers)
{
    const auto & implementation = getResourceImplementationLimits();
    EffectiveResourceLimits result;
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        result.values[index] = implementation.get(static_cast<ResourceLimit>(index));
        result.binding_layers[index] = ResourceLimitLayerKind::Implementation;
    }

    for (const auto & layer : layers)
    {
        if (!isKnownLayer(layer.getKind()) || layer.getKind() == ResourceLimitLayerKind::Implementation)
            throw ResourceLimitError(ResourceLimitError::Code::InvalidLayer, "resource limit override layer is invalid");

        for (std::size_t index = 0; index < resource_limit_count; ++index)
        {
            const auto limit = static_cast<ResourceLimit>(index);
            const auto candidate = layer.get(limit);
            if (!candidate)
                continue;
            if (*candidate > implementation.get(limit))
            {
                String message = "resource limit '";
                message.append(resourceLimitName(limit));
                message.append("' from layer '");
                message.append(resourceLimitLayerName(layer.getKind()));
                message.append("' exceeds the implementation maximum");
                throw ResourceLimitError(ResourceLimitError::Code::ExceedsImplementationMaximum, message);
            }
            if (*candidate < result.values[index]
                || (*candidate == result.values[index] && layerPriority(layer.getKind()) > layerPriority(result.binding_layers[index])))
            {
                result.values[index] = *candidate;
                result.binding_layers[index] = layer.getKind();
            }
        }
    }
    return result;
}

EffectiveResourceLimits minimumEffectiveResourceLimits(const EffectiveResourceLimits & lhs, const EffectiveResourceLimits & rhs) noexcept
{
    EffectiveResourceLimits result;
    for (std::size_t index = 0; index < resource_limit_count; ++index)
    {
        const auto limit = static_cast<ResourceLimit>(index);
        const UInt64 lhs_value = lhs.get(limit);
        const UInt64 rhs_value = rhs.get(limit);
        const auto lhs_layer = lhs.getBindingLayer(limit);
        const auto rhs_layer = rhs.getBindingLayer(limit);
        if (rhs_value < lhs_value || (rhs_value == lhs_value && layerPriority(rhs_layer) > layerPriority(lhs_layer)))
        {
            result.values[index] = rhs_value;
            result.binding_layers[index] = rhs_layer;
        }
        else
        {
            result.values[index] = lhs_value;
            result.binding_layers[index] = lhs_layer;
        }
    }
    return result;
}

static_assert(resource_limit_count == 47);
static_assert(resource_limit_contract_abi != 0);

}
